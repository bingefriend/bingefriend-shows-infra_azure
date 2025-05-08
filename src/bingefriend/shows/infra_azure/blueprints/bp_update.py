"""Azure Durable Functions blueprint for processing TV Maze show updates via Timer (Sequential Processing)."""

import logging
from typing import Any, List, Dict
import time
# noinspection PyPackageRequirements
import azure.functions as func
import azure.durable_functions as df
from bingefriend.shows.application.services.show_service import ShowService
from bingefriend.shows.application.repositories.database import SessionLocal
from bingefriend.shows.client_tvmaze.tvmaze_api import TVMazeAPI
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from sqlalchemy.orm import Session

bp = df.Blueprint()


# --- Timer Trigger Client ---
@bp.timer_trigger(schedule="0 0 5 * * *",
                  arg_name="timer",
                  run_on_startup=False,
                  use_monitor=True)
@bp.durable_client_input(client_name="client")
async def DailyUpdateTimerClient(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    """
    Timer trigger to start the Show Update Orchestration daily.
    """
    if timer.past_due:
        logging.warning('The timer is past due!')

    update_period = 'day'
    orchestrator_input = {"period": update_period}

    try:
        instance_id = await client.start_new("ShowUpdateOrchestrator", client_input=orchestrator_input)
        logging.info(
            f"Started Show Update Orchestration for period '{update_period}' via timer with ID = '{instance_id}'.")

    except Exception as e:
        logging.exception(f"Error starting show update orchestration from timer: {e}")


# --- Orchestrator ---
# noinspection PyPep8Naming
@bp.orchestration_trigger(context_name="context")
def ShowUpdateOrchestrator(context: df.DurableOrchestrationContext) -> Any:
    """
    Orchestrates fetching and processing updated show data from TVMaze API sequentially.
    Takes input like {"period": "day"}
    """
    orchestrator_input = context.get_input() or {}
    period = orchestrator_input.get("period", "day")
    total_shows_processed_count = 0
    total_shows_failed_count = 0

    context.set_custom_status(f"Orchestration started. Fetching updates for the last '{period}'...")
    logging.info(f"ShowUpdateOrchestrator: Started for period '{period}'.")

    # 1. Fetch updated show IDs
    updated_show_ids_dict: Dict[str, int] | None = yield context.call_activity(
        "FetchShowUpdatesActivity", {"period": period}
    )

    if updated_show_ids_dict is None:
        error_msg = f"Failed to fetch show updates for period '{period}'. Orchestration stopping."
        context.set_custom_status(error_msg)
        logging.error(error_msg)
        return {"status": "Failed", "reason": "Could not fetch updates."}

    if not updated_show_ids_dict:
        success_msg = f"No show updates found for the last '{period}'. Orchestration complete."
        context.set_custom_status(success_msg)
        logging.info(success_msg)
        return {"status": "Completed", "message": "No updates found."}

    show_ids_to_process: List[int] = [int(show_id) for show_id in updated_show_ids_dict.keys()]
    total_updates_found = len(show_ids_to_process)
    context.set_custom_status(
        f"Found {total_updates_found} updated shows for period '{period}'. Starting sequential processing...")
    logging.info(f"ShowUpdateOrchestrator: Found {total_updates_found} updated shows. Processing sequentially.")

    # 2. Process each updated show sequentially to manage API calls within ProcessShowRecordActivity
    for i, show_id in enumerate(show_ids_to_process):
        context.set_custom_status(
            f"Processing show {i + 1}/{total_updates_found} (ID: {show_id})... Fetching summary...")
        logging.info(f"Processing show {i + 1}/{total_updates_found} (ID: {show_id})")

        # 2a. Fetch basic show summary data for the updated show ID
        show_summary_record: Dict[str, Any] | None = yield context.call_activity(
            "FetchShowSummaryActivity", {"show_id": show_id}
        )

        if not isinstance(show_summary_record, dict):
            logging.error(f"Failed to fetch summary or got invalid data for show_id {show_id}. Skipping processing.")
            total_shows_failed_count += 1
            continue  # Move to the next show ID

        # 2b. Process this show record (which will trigger internal fetches for seasons/episodes)
        context.set_custom_status(f"Processing show {i + 1}/{total_updates_found} (ID: {show_id})... DB Operations...")
        try:
            yield context.call_activity("ProcessShowRecordUpdateActivity", show_summary_record)
            total_shows_processed_count += 1
        except Exception as processing_ex:
            logging.error(
                f"ProcessShowRecordUpdateActivity failed for show_id {show_id} (from orchestrator view): "
                f"{processing_ex}"
            )
            total_shows_failed_count += 1

    # 3. Final status update
    final_status = (f"Update orchestration complete for period '{period}'. "
                    f"Updates Found: {total_updates_found}, "
                    f"Successfully Processed: {total_shows_processed_count}, "
                    f"Failed/Skipped: {total_shows_failed_count}")
    context.set_custom_status(final_status)
    logging.info(final_status)

    return {
        "status": "Completed",
        "period": period,
        "updates_found": total_updates_found,
        "processed_successfully": total_shows_processed_count,
        "failed_or_skipped": total_shows_failed_count
    }


# --- Activity Functions ---

@bp.activity_trigger(input_name="params")
def FetchShowUpdatesActivity(params: dict) -> Dict[str, int] | None:
    """Fetches recently updated show IDs from the TVMaze API."""
    period = params.get("period", "day")
    logging.info(f"FetchShowUpdatesActivity: Fetching show updates for period '{period}'.")
    try:
        tvmaze_api = TVMazeAPI()
        updates = tvmaze_api.get_show_updates(period=period)
        if updates is None:
            logging.error(f"FetchShowUpdatesActivity: Received None or invalid format for updates (period: {period}).")
            return None
        logging.info(f"FetchShowUpdatesActivity: Found {len(updates)} updates for period '{period}'.")
        return updates
    except Exception as e:
        logging.exception(f"Error in FetchShowUpdatesActivity for period '{period}': {e}")
        return None


@bp.activity_trigger(input_name="params")
def FetchShowSummaryActivity(params: dict) -> Dict[str, Any] | None:
    """
    Fetches basic details/summary data for a single show, making ONE API call.
    """
    show_id = params.get("show_id")
    if not show_id:
        logging.error("FetchShowSummaryActivity: show_id not provided in params.")
        return None

    logging.info(f"FetchShowSummaryActivity: Fetching summary details for show_id {show_id}")
    try:
        tvmaze_api = TVMazeAPI()
        show_summary_data = tvmaze_api.get_show_details(show_id)

        if show_summary_data:
            logging.info(f"FetchShowSummaryActivity: Successfully fetched summary data for show_id {show_id}")
        else:
            logging.warning(f"FetchShowSummaryActivity: No summary data returned from API for show_id {show_id}")
            return None
        return show_summary_data
    except Exception as e:
        logging.exception(f"Error in FetchShowSummaryActivity for show_id {show_id}: {e}")
        return None


# noinspection PyPep8Naming,PyUnboundLocalVariable
@bp.activity_trigger(input_name="record")  # 'record' is now the show_summary_data
def ProcessShowRecordUpdateActivity(record: Dict[str, Any]) -> None:
    """
    Process/Update a single show record (basic summary data).
    The ShowService called internally will fetch required season/episode data via API.
    Includes deadlock retry logic.
    """
    show_id_for_log = record.get('id', 'Unknown')
    show_name = record.get('name', f"ID: {show_id_for_log}")

    MAX_DEADLOCK_RETRIES = 3
    RETRY_DELAY_SECONDS = 2

    last_exception = None

    for attempt in range(MAX_DEADLOCK_RETRIES):
        db: Session | None = None
        try:
            db = SessionLocal()
            show_service = ShowService()

            show_service.process_show_record(record, db)

            db.commit()
            logging.info(
                f"ProcessShowRecordActivity: Successfully processed and committed show: '{show_name}' on attempt "
                f"{attempt + 1}"
            )
            return

        except SQLAlchemyOperationalError as op_err:
            if db:
                db.rollback()
            last_exception = op_err
            original_exception = getattr(op_err, 'orig', None)
            error_code = 0
            if original_exception and hasattr(original_exception, 'args') and isinstance(original_exception.args,
                                                                                         tuple) and len(
                    original_exception.args) > 0:
                error_code = original_exception.args[0]

            if error_code == 1213 and attempt < MAX_DEADLOCK_RETRIES - 1:
                logging.warning(
                    f"Deadlock detected processing show '{show_name}' (ID: {show_id_for_log}) on "
                    f"attempt {attempt + 1}/{MAX_DEADLOCK_RETRIES}. Retrying after delay..."
                )
            else:
                logging.error(
                    f"SQLAlchemyOperationalError on final attempt or non-deadlock error for show '{show_name}' (ID: "
                    f"{show_id_for_log}): {op_err}"
                )
                break

        except Exception as e:
            if db:
                db.rollback()
            last_exception = e
            logging.exception(
                f"Non-operational error processing show record: '{show_name}' (ID: {show_id_for_log}). Error: {e}")
            break

        finally:
            if db:
                db.close()

        if 'error_code' in locals() and error_code == 1213:
            sleep_time = RETRY_DELAY_SECONDS * (attempt + 1)
            time.sleep(sleep_time)
            logging.info(f"Retrying processing for show '{show_name}' (ID: {show_id_for_log}), attempt {attempt + 2}")

    logging.error(
        f"ProcessShowRecordActivity: Failed to process show '{show_name}' (ID: {show_id_for_log}) after {attempt + 1} "
        f"attempts."
    )
    if last_exception:
        raise last_exception
    else:
        raise Exception(f"Failed to process show {show_name} (ID: {show_id_for_log}) after max retries.")
