"""Service for episode-related operations."""

from typing import Any
from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI
from bingefriend.shows.infra_azure.repositories.episode_repo import EpisodeRepository
from bingefriend.shows.infra_azure.services.season_service import SeasonService


# noinspection PyMethodMayBeStatic
class EpisodeService:
    """Service for episode-related operations."""

    def fetch_episode_index_page(self, show_id: int) -> list[dict[str, Any]]:
        """Fetch all episodes for a given show_id from the external API.

        Args:
            show_id (int): The ID of the show to fetch episodes for.

        Returns:
            dict: A dictionary containing the episodes data.

        """

        tvmaze_api = TVMazeAPI()

        show_episodes = tvmaze_api.get_episodes(show_id)

        if not show_episodes:
            raise ValueError(f"No episodes found for show_id: {show_id}")
        return show_episodes

    def process_episode_record(self, record: dict, show_id: int) -> None:
        """Process a single episode record and store it in the database.

        Args:
            record (dict): The episode record to process.
            show_id (int): The ID of the show to associate with the episode.

        """

        record["show_id"] = show_id

        # Get season ID for the episode
        season_number = record.get("season")
        record['season_id'] = SeasonService().get_season_id_by_show_id_and_number(
            show_id=show_id, season_number=season_number
        )

        # Process the episode record
        episode_repo = EpisodeRepository()
        episode_repo.create_episode(record)
