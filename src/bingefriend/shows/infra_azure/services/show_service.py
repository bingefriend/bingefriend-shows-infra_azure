"""Service to handle the ingestion of show data from an external API."""

import logging
from typing import Any
from bingefriend.shows.infra_azure.repositories.show_repo import ShowRepository
from bingefriend.shows.infra_azure.services.network_service import NetworkService
from bingefriend.shows.infra_azure.services.show_genre_service import ShowGenreService
from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI


# noinspection PyMethodMayBeStatic
class ShowService:
    """Service to handle the ingestion of show data from an external API."""
    def __init__(self):
        self.show_repo = ShowRepository()
        self.network_service = NetworkService()
        self.genre_service = ShowGenreService()

    def fetch_show_index_page(self, page_number: int) -> dict[str, Any] | None:
        """Fetch a page of shows from the external API and enqueue the next page."""

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

    def process_show_record(self, record: dict[str, Any]) -> int:
        """Process a single show record."""

        # Process network data from the record
        network_id = record.get('network').get('id')

        if network_id:
            record['network_id'] = self.network_service.get_or_create_network(network_id)

        # Process the show record
        show_id = self.show_repo.create_show(record)

        return show_id
