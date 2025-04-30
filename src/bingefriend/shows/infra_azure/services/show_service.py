"""Service to handle the ingestion of show data from an external API."""

import logging
from typing import Any
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

    def process_show_record(self, record: dict[str, Any]) -> None:
        """Process a single show record.

        Args:
            record (dict[str, Any]): The show record to process.

        """

        # Process network data from the record
        network_service = NetworkService()
        network_info = record.get('network')

        if network_info:
            record['network_id'] = network_service.get_or_create_network(network_info)

        # Process web channel data from the record
        web_channel_service = WebChannelService()
        web_channel_info = record.get('webChannel')

        if web_channel_info:
            record['web_channel_id'] = web_channel_service.get_or_create_web_channel(web_channel_info)

        # Process the show record
        show_repo = ShowRepository()
        show_id: int = show_repo.create_show(record)

        # Process show genres
        genres = record.get('genres', [])

        if genres:
            for genre in genres:
                genre_service = GenreService()
                genre_id = genre_service.get_or_create_genre(genre)

                show_genre_service = ShowGenreService()
                show_genre_service.create_show_genre(show_id, genre_id)

        # Process show seasons
        season_service = SeasonService()
        seasons = season_service.fetch_season_index_page(record.get('id'))

        if seasons:
            for season in seasons:
                season_service.process_season_record(season, show_id)

        # Process show episodes
        episode_service = EpisodeService()
        episodes = episode_service.fetch_episode_index_page(record.get('id'))

        if episodes:
            for episode in episodes:
                episode_service.process_episode_record(episode, show_id)
