"""Start a complete ingestion of TV Maze dataset."""

import logging
from typing import Any, Generator
# noinspection PyPackageRequirements
import azure.functions as func
import azure.durable_functions as df
from azure.durable_functions.models.Task import TaskBase
from bingefriend.shows.infra_azure import config as config
from bingefriend.shows.infra_azure.services.episode_service import EpisodeService
from bingefriend.shows.infra_azure.services.genre_service import GenreService
from bingefriend.shows.infra_azure.services.show_genre_service import ShowGenreService
from bingefriend.shows.infra_azure.services.show_service import ShowService
from bingefriend.shows.infra_azure.services.season_service import SeasonService

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
        instance_id = await client.start_new("IngestOrchestrator")
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

    Orchestrates the multi-stage ingestion process for shows, seasons, and episodes.
    1. Fetches paginated show index.
    2. For each show record:
        a. Processes the show record.
        b. Starts a sub-orchestration for that show's seasons and episodes.
    3. Waits for all sub-orchestrations to complete.

    Args:
        context (df.DurableOrchestrationContext): The Durable Functions context.

    Returns:
        dict: A dictionary containing the count of launched show processing tasks.

    """
    current_show_page = 0
    all_show_processing_tasks = []

    while current_show_page is not None:
        # A. Fetch one page of shows
        page_result = yield context.call_activity("FetchShowIndexPageActivity", {"page_number": current_show_page})

        if not page_result or not page_result.get("records"):
            break  # Exit loop if no records or error

        # B. Process each show record and immediately launch its sub-orchestration
        for record in page_result.get("records", []):
            processed_show_info = yield context.call_activity("ProcessShowRecordActivity", record)

            # Assuming the activity returns the processed record or None on failure
            # And the record contains the ID needed. Adjust 'id' key if necessary.
            show_id: int = processed_show_info.get("show_id") if processed_show_info else None
            genres: list[str] = processed_show_info.get("genres")

            if show_id:
                # 2. Start the sub-orchestration for this specific show
                sub_task = context.call_sub_orchestrator(
                    "ProcessSingleShowOrchestrator",
                    {"show_id": show_id, "genres": genres}
                )
                all_show_processing_tasks.append(sub_task)
            else:
                logging.warning(
                    f"Skipping season/episode processing for a show due to processing error or missing ID in record: "
                    f"{record.get('id', 'Unknown ID')}"
                )

        current_show_page = page_result.get("next_page")

    # III. Wait for all launched sub-orchestrations to complete
    if all_show_processing_tasks:
        yield context.task_all(all_show_processing_tasks)

    return {"launched_show_processing_tasks": len(all_show_processing_tasks)}


# --- Sub-Orchestrator ---
@bp.orchestration_trigger(context_name="context")
def ProcessSingleShowOrchestrator(context: df.DurableOrchestrationContext) -> Generator[TaskBase, Any, None]:
    """Processes seasons and then episodes for a single show."""
    params = context.get_input()
    show_id = params.get("show_id")
    genres = params.get("genres")

    if not show_id:
        logging.error("ProcessSingleShowOrchestrator called without show_id.")
        return

    # A. Process genres for this show_id
    if genres:
        processing_genres = []
        task = context.call_activity("ProcessShowGenresActivity", {"show_id": show_id, "genres": genres})
        processing_genres.append(task)

        if processing_genres:
            yield context.task_all(processing_genres)
    else:
        logging.warning(f"No genres found for show_id: {show_id}")

    # A. Fetch and process seasons for this show_id
    seasons_for_show = yield context.call_activity("FetchSeasonsForShowActivity", {"show_id": show_id})

    if seasons_for_show and seasons_for_show.get("records"):
        processing_seasons = []
        for season_record in seasons_for_show.get("records", []):
            season_record_with_show_id = season_record.copy()
            season_record_with_show_id["show_id"] = show_id
            task = context.call_activity("ProcessSeasonRecordActivity", season_record_with_show_id)
            processing_seasons.append(task)

        if processing_seasons:
            yield context.task_all(processing_seasons)
    else:
        logging.warning(f"No seasons found or error fetching seasons for show_id: {show_id}")

    # B. Fetch and process episodes for this show_id (only after seasons are done)
    episodes_for_show = yield context.call_activity("FetchEpisodesForShowActivity", {"show_id": show_id})

    if episodes_for_show and episodes_for_show.get("records"):
        processing_episodes = []
        for episode_record in episodes_for_show.get("records", []):
            episode_record_with_show_id = episode_record.copy()
            episode_record_with_show_id["show_id"] = show_id
            task = context.call_activity("ProcessEpisodeRecordActivity", episode_record_with_show_id)
            processing_episodes.append(task)

        if processing_episodes:
            yield context.task_all(processing_episodes)
    else:
        logging.warning(f"No episodes found or error fetching episodes for show_id: {show_id}")


# --- Activity Functions ---
@bp.activity_trigger(input_name="params")
def FetchShowIndexPageActivity(params: dict):
    """Fetch a page of shows from the external API and enqueue the next page."""

    show_records = ShowService().fetch_show_index_page(page_number=params["page_number"])

    return show_records


@bp.activity_trigger(input_name="record")
def ProcessShowRecordActivity(record: dict) -> dict[str, Any]:
    """Ingest a single show record into the database."""

    show_id = ShowService().process_show_record(record)
    genre_data = {'show_id': show_id, 'genres': record.get('genres', [])}

    return genre_data


@bp.activity_trigger(input_name="params")
def ProcessShowGenresActivity(params: dict) -> None:
    """Process genres for a show record."""

    show_id = params["show_id"]
    genres = params["genres"]

    if not genres:
        logging.warning(f"No genres found for show_id: {show_id}")
        return

    for genre in genres:
        genre_id = GenreService().get_or_create_genre(genre)

        if genre_id:
            ShowGenreService().create_show_genre(show_id, genre_id)


@bp.activity_trigger(input_name="params")
def FetchSeasonsForShowActivity(params: dict) -> dict[str, Any]:
    """Fetch all seasons for a given show_id from the external API."""

    show_seasons = SeasonService().fetch_season_index_page(show_id=params["show_id"])

    return show_seasons


# noinspection PyTypeChecker
@bp.activity_trigger(input_name="record")
@bp.sql_output(
    arg_name="seasonsql",
    connection_string_setting=config.AZURE_SQL_CONNECTION_STRING,
    command_text=config.SEASONS_TABLE
)
def ProcessSeasonRecordActivity(record: dict, seasonsql: func.Out[func.SqlRow]) -> None:
    """Process a single season record and store it in the database."""

    seasonsql.set(SeasonService().process_season_record(record))


@bp.activity_trigger(input_name="params")
def FetchEpisodesForShowActivity(params: dict):
    """Fetch all episodes for a given show_id from the external API."""
    show_id = params["show_id"]
    show_episodes = EpisodeService().fetch_episode_index_page(show_id=show_id)

    return show_episodes


# noinspection PyTypeChecker
@bp.activity_trigger(input_name="recordinfo")
@bp.sql_output(
    arg_name="episodesql",
    connection_string_setting=config.AZURE_SQL_CONNECTION_STRING,
    command_text=config.EPISODES_TABLE
)
def ProcessEpisodeRecordActivity(recordinfo: dict, episodesql: func.Out[func.SqlRow]) -> None:
    """Process a single episode record and store it in the database."""

    episodesql.set(EpisodeService().process_episode_record(recordinfo))
