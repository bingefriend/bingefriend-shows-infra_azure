"""Blueprint for updating shows based on the TVMaze updates endpoint."""

import logging
import os  # Import os module
from typing import Any, List, Dict
# noinspection PyPackageRequirements
import azure.functions as func
import azure.durable_functions as df
# Assuming ShowService has methods to fetch updates and process a single show
from bingefriend.shows.infra_azure.services.show_service import ShowService

bp = df.Blueprint()

# Define how many show updates to process concurrently
# Adjust based on observed API limits or desired throughput
CONCURRENT_UPDATE_LIMIT = 10

# --- Timer Trigger Client ---
# Reads the CRON schedule from the environment variable 'UPDATE_ORCHESTRATOR_TIMER_SCHEDULE'
# Defaults to '0 0 * * * *' (every hour at the start of the hour) if the variable is not set.
UPDATE_SCHEDULE = os.environ.get("UPDATE_ORCHESTRATOR_TIMER_SCHEDULE", "0 0 * * * *")


# noinspection PyUnusedLocal,PyBroadException
@bp.timer_trigger(schedule=UPDATE_SCHEDULE, arg_name="timer", run_on_startup=False)
@bp.durable_client_input(client_name="client")
async def UpdateTimerClient(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    """
    Timer-triggered function that starts the UpdateOrchestrator periodically.
    """
    logging.info(f"UpdateTimerClient triggered by schedule: {UPDATE_SCHEDULE}")
    try:
        instance_id = await client.start_new("UpdateOrchestrator")
        logging.info(f"Started update orchestration with ID = '{instance_id}' triggered by timer.")

    except Exception as e:
        logging.exception("Error starting update orchestration from timer trigger.")
        # Depending on monitoring setup, you might want to raise the exception
        # or handle it specifically (e.g., send an alert).


# --- Orchestrator ---
@bp.orchestration_trigger(context_name="context")
def UpdateOrchestrator(context: df.DurableOrchestrationContext) -> Any:
    """
    Orchestrates fetching show updates and processing each updated show.
    """
    logging.info("UpdateOrchestrator started.")
    all_results = []

    try:
        # A. Fetch the list of show IDs that have been updated
        # The result is expected to be a dictionary {show_id: timestamp}
        # We only need the keys (show IDs) for processing.
        update_list_result: Dict[str, int] = yield context.call_activity("FetchUpdateListActivity")

        if not update_list_result:
            logging.info("No show updates found or error fetching update list. Ending orchestration.")
            return []  # Nothing to process

        show_ids_to_update: List[str] = list(update_list_result.keys())
        total_updates = len(show_ids_to_update)
        logging.info(f"Found {total_updates} shows to update. Processing in batches of {CONCURRENT_UPDATE_LIMIT}.")

        # B. Process show updates in batches to limit concurrency
        for i in range(0, total_updates, CONCURRENT_UPDATE_LIMIT):
            batch_tasks: List[Any] = []
            current_batch_ids = show_ids_to_update[i:i + CONCURRENT_UPDATE_LIMIT]
            logging.info(
                f"Scheduling update batch {i // CONCURRENT_UPDATE_LIMIT + 1} with {len(current_batch_ids)} shows.")

            for show_id in current_batch_ids:
                # Pass the show ID to the processing activity
                batch_tasks.append(context.call_activity("ProcessShowUpdateActivity", {"show_id": show_id}))

            # C. Wait for the current batch of update tasks to complete
            if batch_tasks:
                batch_processing_results = yield context.task_all(batch_tasks)
                all_results.extend(batch_processing_results)  # Optional: Store results if activities return values
                logging.info(f"Finished processing update batch {i // CONCURRENT_UPDATE_LIMIT + 1}.")

            # Optional: Add a small delay between batches if needed to further reduce load
            # yield context.create_timer(context.current_utc_datetime + timedelta(seconds=1))

        logging.info(f"Successfully processed all {total_updates} show updates.")

    except Exception as e:
        logging.error(f"Update orchestration failed: {e}", exc_info=True)
        # Handle orchestration errors appropriately
        # Depending on requirements, you might want to signal failure or just log

    return all_results  # Return collected results if needed


# --- Activity Functions ---
# noinspection PyUnusedLocal
@bp.activity_trigger(input_name="payload")  # No input needed for fetching updates
def FetchUpdateListActivity(payload: Any) -> Dict[str, int]:
    """
    Fetches the list of updated show IDs from the external API (e.g., TVMaze /updates/shows).
    """
    logging.info("Executing FetchUpdateListActivity.")
    try:
        show_service = ShowService()
        # Assuming ShowService has a method like this
        update_list = show_service.fetch_show_updates()
        logging.info(f"Fetched {len(update_list)} items from the update list.")
        return update_list
    except Exception as e:
        logging.error(f"Error in FetchUpdateListActivity: {e}", exc_info=True)
        # Return empty dict or raise to signal failure
        return {}


@bp.activity_trigger(input_name="update_info")
def ProcessShowUpdateActivity(update_info: dict) -> None:
    """
    Processes a single show update. Fetches full show details using the show_id
    and updates the database (show, seasons, episodes, etc.).
    """
    show_id = update_info.get("show_id")
    if not show_id:
        logging.error("ProcessShowUpdateActivity called without a show_id.")
        return  # Or raise an error

    logging.info(f"Executing ProcessShowUpdateActivity for show ID: {show_id}.")
    try:
        show_service = ShowService()
        # Reuse or adapt the logic from the ingest process.
        # This likely involves fetching the full show details by ID first,
        # then calling a method similar to process_show_record.
        # Example: Fetch full record first
        show_record = show_service.fetch_show_details(show_id)  # Assumes this method exists
        if show_record:
            # Reuse the existing processing logic which handles create/update
            show_service.process_show_record(show_record)
            logging.info(f"Successfully processed update for show ID: {show_id}")
        else:
            logging.warning(f"Could not fetch details for updated show ID: {show_id}. Skipping.")

    except Exception as e:
        logging.error(f"Error processing update for show ID {show_id}: {e}", exc_info=True)
        # Decide if the error should halt the orchestration or just be logged
        # Re-raising the exception here will cause the orchestrator to see the activity as failed.
        # raise e
