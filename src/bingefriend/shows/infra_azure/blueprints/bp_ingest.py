"""Start a complete ingestion of TV Maze dataset."""

import logging
from typing import Any
# noinspection PyPackageRequirements
import azure.functions as func
import azure.durable_functions as df
from bingefriend.shows.application.services.show_service import ShowService

bp = df.Blueprint()


# --- HTTP Trigger Client ---
@bp.route(route="ingest", auth_level="function", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def IngestClient(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Start Ingest function.

    Args:
        req (func.HttpRequest): The HTTP request object.
        client (df.DurableOrchestrationClient): The Durable Functions client.

    Returns:
        func.HttpResponse: The HTTP response object.

    """
    try:
        # Pass initial parameters if needed, e.g., start_page
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

    Orchestrates the ingestion of show data from an external API.

    Args:
        context (df.DurableOrchestrationContext): The Durable Functions orchestration context.

    Returns:
        Any: The result of the orchestration.

    """

    current_show_page = 0

    while current_show_page is not None:
        # A. Fetch one page of shows
        page_result = yield context.call_activity("FetchShowIndexPageActivity", {"page_number": current_show_page})

        if not page_result or not page_result.get("records"):
            break  # Exit loop if no records or error

        # B. Process each show record and immediately launch its sub-orchestration
        for record in page_result.get("records", []):
            yield context.call_activity("ProcessShowRecordActivity", record)

        current_show_page = page_result.get("next_page")


# --- Activity Functions ---
@bp.activity_trigger(input_name="params")
def FetchShowIndexPageActivity(params: dict):
    """Fetch a page of shows from the external API and enqueue the next page."""

    show_records = ShowService().fetch_show_index_page(page_number=params["page_number"])

    logging.info(f"Fetched show records for page {params['page_number']}")

    return show_records


@bp.activity_trigger(input_name="record")
def ProcessShowRecordActivity(record: dict) -> None:
    """Ingest a single show record into the database."""

    ShowService().process_show_record(record)

    logging.info(f"Processed show record: {record.get('name')}")
