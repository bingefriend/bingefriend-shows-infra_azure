"""Ingest Blueprint for Azure Durable Functions."""

import logging
from datetime import timedelta
from typing import Any, List
# noinspection PyPackageRequirements
import azure.functions as func
import azure.durable_functions as df
from bingefriend.shows.infra_azure.services.show_service import ShowService

bp = df.Blueprint()


# --- HTTP Trigger Client ---
@bp.route(route="ingest", auth_level="function", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def IngestClient(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Start Ingest function."""
    try:
        instance_id = await client.start_new("ShowOrchestrator")
        logging.info(f"Started orchestration with ID = '{instance_id}'.")
        response_body = client.create_check_status_response(req, instance_id)
        return func.HttpResponse(
            body=response_body.get_body(),
            status_code=202,
            mimetype="application/json",
        )
    except Exception as e:
        logging.exception("Error starting orchestration.")
        return func.HttpResponse(f"An unexpected error occurred: {e}", status_code=500)


# --- Orchestrator ---
@bp.orchestration_trigger(context_name="context")
def ShowOrchestrator(context: df.DurableOrchestrationContext) -> Any:
    """ShowOrchestrator function.

    Orchestrates the ingestion of show data from an external API, processing shows in parallel per page,
    with limited concurrency per batch.

    Args:
        context (df.DurableOrchestrationContext): The Durable Functions orchestration context.

    Returns:
        Any: The result of the orchestration.

    """
    # Define how many activities to run concurrently
    # Adjust this based on observed API limits or desired throughput
    CONCURRENT_ACTIVITY_LIMIT = 10

    current_show_page = 0
    all_results = []

    while current_show_page is not None:
        # A. Fetch one page of shows
        page_result = yield context.call_activity("FetchShowIndexPageActivity", {"page_number": current_show_page})

        if not page_result or not page_result.get("records"):
            logging.info(f"No more records found or error fetching page {current_show_page}. Ending orchestration.")
            break

        records_on_page = page_result.get("records", [])
        logging.info(f"Processing {len(records_on_page)} records for page {current_show_page} in batches of "
                     f"{CONCURRENT_ACTIVITY_LIMIT}.")

        # B. Process records in batches to limit concurrency
        for i in range(0, len(records_on_page), CONCURRENT_ACTIVITY_LIMIT):
            batch_tasks: List[Any] = []
            current_batch = records_on_page[i:i + CONCURRENT_ACTIVITY_LIMIT]
            logging.info(f"Scheduling batch {i // CONCURRENT_ACTIVITY_LIMIT + 1} with {len(current_batch)} records.")

            for record in current_batch:
                batch_tasks.append(context.call_activity("ProcessShowRecordActivity", record))

            # C. Wait for the current batch of tasks to complete
            if batch_tasks:
                batch_processing_results = yield context.task_all(batch_tasks)
                all_results.extend(batch_processing_results)  # Optional: Store results
                logging.info(
                    f"Finished processing batch {i // CONCURRENT_ACTIVITY_LIMIT + 1} for page {current_show_page}.")
            yield context.create_timer(context.current_utc_datetime + timedelta(seconds=1))

        # D. Determine the next page
        current_show_page = page_result.get("next_page")
        if current_show_page is not None:
            logging.info(f"Moving to next page: {current_show_page}")
        else:
            logging.info("Reached the last page.")

    return all_results


# --- Activity Functions ---
@bp.activity_trigger(input_name="params")
def FetchShowIndexPageActivity(params: dict):
    """Fetch a page of shows from the external API."""
    # Instantiate dependencies needed for this activity
    # Consider dependency injection if service becomes complex
    show_service = ShowService()
    show_records = show_service.fetch_show_index_page(page_number=params["page_number"])
    # Avoid logging potentially large data structures in production
    # logging.info(f"Fetched show records for page {params['page_number']}")
    return show_records


@bp.activity_trigger(input_name="record")
def ProcessShowRecordActivity(record: dict) -> None:
    """Ingest a single show record into the database."""
    # Instantiate dependencies needed for this activity
    show_service = ShowService()
    try:
        show_service.process_show_record(record)
        # Log success sparingly, e.g., just the ID or name if needed
        # logging.info(f"Successfully processed show record Maze ID: {record.get('id')}")
    except Exception as e:
        # Log errors with details
        logging.error(f"Error processing show record Maze ID {record.get('id')}: {e}", exc_info=True)
        # Decide if the error should halt the orchestration or just be logged
        # Re-raising the exception here will cause the orchestrator to see the activity as failed.
        # raise e # Uncomment if an activity failure should potentially fail the orchestration
