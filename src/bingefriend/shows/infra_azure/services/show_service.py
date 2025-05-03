"""Service to handle the ingestion of show data from an external API."""

import logging
from typing import Any

from sqlalchemy.orm import Session

from bingefriend.shows.infra_azure.repositories.show_repo import ShowRepository
from bingefriend.shows.infra_azure.services.episode_service import EpisodeService
from bingefriend.shows.infra_azure.services.genre_service import GenreService
from bingefriend.shows.infra_azure.services.network_service import NetworkService
from bingefriend.shows.infra_azure.services.season_service import SeasonService
from bingefriend.shows.infra_azure.services.show_genre_service import ShowGenreService
from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI
from bingefriend.shows.infra_azure.services.web_channel_service import WebChannelService


# noinspection PyMethodMayBeStatic
class ShowService:
    """Service to handle the ingestion of show data from an external API."""

    def fetch_show_index_page(self, page_number: int) -> dict[str, Any] | None:
        """Fetch a page of shows from the external API and enqueue the next page.

        Args:
            page_number (int): The page number to fetch.

        Returns:
            dict[str, Any] | None: A dictionary containing the shows and the next page number, or None if no more pages.

        """

        try:
            tvmaze_api = TVMazeAPI()
            shows_summary = tvmaze_api.get_shows(page=page_number)

            # Handle None response (404 or permanent API error after retries)
            if shows_summary is None:
                logging.info(f"API returned None (404 likely) for page {page_number}. Stopping pagination.")
                return None  # End of pages or API issue handled by caller/retry

            # Handle empty list (valid end of pagination)
            if not shows_summary:
                logging.info(f"No shows found on page {page_number}. Ending pagination.")
                return None  # End of pages
        except Exception as init_err:
            logging.exception(f"Failed to get dependencies for ingestion service: {init_err}")
            raise init_err  # Cannot proceed

        return {
            'records': shows_summary,
            'next_page': page_number + 1 if shows_summary else None
        }

    def process_show_record(self, record: dict[str, Any], db: Session) -> None:
        """Process a single show record, creating or updating it and its related entities.

        Args:
            record (dict[str, Any]): The show record data from the API.
            db (Session): The database session to use for database operations.

        """
        show_maze_id = record.get('id')
        if not show_maze_id:
            logging.error("Show record is missing 'id' (maze_id). Skipping processing.")
            return

        logging.info(f"Processing show record for maze_id: {show_maze_id}")

        # --- Network & Web Channel ---
        # Get or create Network/WebChannel and add their IDs to the record dict
        # for the ShowRepository. This part remains the same.
        network_service = NetworkService()
        network_info = record.get('network')
        record['network_id'] = network_service.get_or_create_network(network_info, db) if network_info else None

        web_channel_service = WebChannelService()
        web_channel_info = record.get('webChannel')
        record['web_channel_id'] = web_channel_service.get_or_create_web_channel(
            web_channel_info, db) if web_channel_info else None

        # --- Show Upsert ---
        # Assume ShowRepository has an upsert method that finds by maze_id,
        # updates if found, creates if not, and returns the internal DB show_id.
        show_repo = ShowRepository()
        # *** Requires ShowRepository.upsert_show implementation ***
        show_id: int | None = show_repo.upsert_show(record, db)

        if show_id is None:
            logging.error(
                f"Failed to create or update show with maze_id: {show_maze_id}. Aborting further processing for this "
                f"show."
            )
            return  # Cannot proceed without a valid show_id

        logging.info(f"Successfully upserted show maze_id: {show_maze_id}, internal DB ID: {show_id}")

        # --- Genres ---
        # Process genres: Get/create genre IDs and sync associations.
        genre_names = record.get('genres', [])
        genre_ids = []
        if genre_names:
            genre_service = GenreService()
            for genre_name in genre_names:
                genre_id = genre_service.get_or_create_genre(genre_name, db)
                if genre_id:
                    genre_ids.append(genre_id)

        # Assume ShowGenreService has a method to synchronize genres for a show.
        # This method would add new links and remove outdated ones.
        show_genre_service = ShowGenreService()
        # *** Requires ShowGenreService.sync_show_genres implementation ***
        show_genre_service.sync_show_genres(show_id, genre_ids, db)
        logging.info(f"Synchronized {len(genre_ids)} genres for show ID: {show_id}")

        # --- Seasons ---
        # Fetch all seasons from API and process each (upsert logic within SeasonService)
        season_service = SeasonService()
        # *** Assumes fetch_season_index_page gets *all* seasons for the show ***
        seasons_data = season_service.fetch_season_index_page(show_maze_id)
        if seasons_data:
            logging.info(f"Processing {len(seasons_data)} seasons for show ID: {show_id}")
            for season_record in seasons_data:
                # *** Requires SeasonService.process_season_record to handle upserts ***
                season_service.process_season_record(season_record, show_id, db)
        else:
            logging.info(f"No seasons found via API for show ID: {show_id}")

        # --- Episodes ---
        # Fetch all episodes from API and process each (upsert logic within EpisodeService)
        episode_service = EpisodeService()
        # *** Assumes fetch_episode_index_page gets *all* episodes for the show ***
        episodes_data = episode_service.fetch_episode_index_page(show_maze_id)
        if episodes_data:
            logging.info(f"Processing {len(episodes_data)} episodes for show ID: {show_id}")
            for episode_record in episodes_data:
                # *** Requires EpisodeService.process_episode_record to handle upserts ***
                episode_service.process_episode_record(episode_record, show_id, db)
        else:
            logging.info(f"No episodes found via API for show ID: {show_id}")

        logging.info(f"Finished processing show record for maze_id: {show_maze_id}")

    def fetch_show_updates(self) -> dict[str, int]:
        """Fetch the list of updated show IDs from the TVMaze API.

        Returns:
            dict[str, int]: A dictionary where keys are show IDs (as strings)
                            and values are the last update timestamp (as int).
                            Returns an empty dictionary if an error occurs.
        """
        logging.info("Fetching show updates from TVMaze API.")
        try:
            tvmaze_api = TVMazeAPI()
            updates = tvmaze_api.get_show_updates()
            if updates is None:
                # Handle cases where the API might return None explicitly on error
                logging.warning("TVMaze API returned None for show updates.")
                return {}
            logging.info(f"Fetched {len(updates)} show updates from API.")
            return updates
        except Exception as e:
            logging.exception(f"Failed to fetch show updates from TVMaze API: {e}")
            # Return empty dict to allow orchestration to continue gracefully
            return {}

    def fetch_show_details(self, show_id: int) -> dict[str, Any] | None:
        """Fetch the full details for a single show from the TVMaze API.

        Args:
            self
            show_id (int): The TVMaze ID of the show to fetch.

        Returns:
            dict[str, Any] | None: A dictionary containing the show details,
                                   or None if the show is not found or an error occurs.
        """
        logging.info(f"Fetching full details for show ID: {show_id} from TVMaze API.")
        try:
            tvmaze_api = TVMazeAPI()
            # Assuming TVMazeAPI has a method like get_show_details(show_id)
            # Adjust the method name if it's different in your TVMazeAPI class
            show_details = tvmaze_api.get_show_details(show_id=show_id)

            if show_details is None:
                # Handle cases where the API might return None (e.g., 404 Not Found)
                logging.warning(f"TVMaze API returned None for show details (ID: {show_id}). Show might not exist.")
                return None

            logging.info(f"Successfully fetched details for show ID: {show_id}.")
            return show_details
        except Exception as e:
            logging.exception(f"Failed to fetch show details for ID {show_id} from TVMaze API: {e}")
            # Return None to indicate failure
            return None
