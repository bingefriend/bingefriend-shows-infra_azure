"""Service to manage network-related operations."""

from typing import Any
from bingefriend.shows.infra_azure.repositories.season_repo import SeasonRepository
from bingefriend.shows.infra_azure.services.network_service import NetworkService
from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI


# noinspection PyMethodMayBeStatic
class SeasonService:
    """Service to handle season-related operations."""

    def fetch_season_index_page(self, show_id: int) -> list[dict[str, Any]]:
        """Fetch a page of seasons for a given show_id from the external API.

        Args:
            show_id (int): The ID of the show to fetch seasons for.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing season data.

        """

        tvmaze_api: TVMazeAPI = TVMazeAPI()
        seasons: list[dict[str, Any]] = tvmaze_api.get_seasons(show_id)
        if not seasons:
            raise ValueError(f"No seasons found for show_id: {show_id}")

        return seasons

    def process_season_record(self, record: dict, show_id: int) -> None:
        """Process a single season record and return the processed data.

        Args:
            record (dict): The season record to process.
            show_id (int): The ID of the show to associate with the season.

        """

        record["show_id"] = show_id

        network_service = NetworkService()

        # Get network data from the record
        if record.get("network"):
            network_id = network_service.get_or_create_network(record["network"])
            record["network_id"] = network_id

        # Process the season record
        season_repo = SeasonRepository()
        season_repo.create_season(record)

    def get_season_id_by_show_id_and_number(self, show_id: int, season_number: int) -> int:
        """Get the season ID for a given show ID and season number.

        Args:
            show_id (int): The ID of the show.
            season_number (int): The season number.

        Returns:
            int: The ID of the season.

        """

        season_repo = SeasonRepository()

        season_id: int = season_repo.get_season_id_by_show_id_and_number(show_id, season_number)

        return season_id
