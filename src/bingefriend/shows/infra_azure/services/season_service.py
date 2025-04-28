"""Service to manage network-related operations."""

from typing import Any
from bingefriend.shows.infra_azure.repositories.season_repo import SeasonRepository
from bingefriend.shows.infra_azure.services.network_service import NetworkService
from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI


# noinspection PyMethodMayBeStatic
class SeasonService:
    """Service to handle season-related operations."""

    def __init__(self):
        self.network_service = NetworkService()
        self.season_repo = SeasonRepository()

    def fetch_season_index_page(self, show_id: int) -> dict[str, Any]:
        """Fetch a page of seasons for a given show_id from the external API."""

        tvmaze_api: TVMazeAPI = TVMazeAPI()
        show_seasons: list[dict[str, Any]] = tvmaze_api.get_seasons(show_id)
        if not show_seasons:
            raise ValueError(f"No seasons found for show_id: {show_id}")

        return {
            'records': show_seasons
        }

    def process_season_record(self, record: dict) -> dict:
        """Process a single season record and return the processed data."""

        # Get network data from the record
        if record.get("network"):
            network_id = self.network_service.get_or_create_network(record["network"])
            record["network_id"] = network_id

        # Process the season record
        processed_season = {
            "maze_id": record.get("id"),
            "url": record.get("url"),
            'number': record.get("number"),
            "name": record.get("name"),
            "episodeOrder": record.get("episodeOrder"),
            "premiereDate": record.get("premiereDate"),
            "endDate": record.get("endDate"),
            "network_id": record.get("network_id"),
            "image_medium": record.get("image", {}).get("medium"),
            "image_original": record.get("image", {}).get("original"),
            "summary": record.get("summary"),
            "show_id": record.get("show_id"),
        }

        return processed_season

    def get_season_id_by_show_id_and_number(self, show_id: int, season_number: int) -> int:
        """Get the season ID for a given show ID and season number."""

        season_id: int = self.season_repo.get_season_id_by_show_id_and_number(show_id, season_number)

        return season_id
