"""Service for episode-related operations."""

from bingefriend.tvmaze_client.tvmaze_api import TVMazeAPI

from bingefriend.shows.infra_azure.services.season_service import SeasonService


# noinspection PyMethodMayBeStatic
class EpisodeService:
    """Service for episode-related operations."""

    def fetch_episode_index_page(self, show_id: int) -> dict:
        """Fetch all episodes for a given show_id from the external API."""

        tvmaze_api = TVMazeAPI()

        show_episodes = tvmaze_api.get_episodes(show_id)

        if not show_episodes:
            raise ValueError(f"No episodes found for show_id: {show_id}")
        return {
            'records': show_episodes
        }

    def process_episode_record(self, record_info: dict) -> dict:
        """Process a single episode record and store it in the database."""

        # Get season ID for the episode
        show_id = record_info.get("show_id")
        season_number = record_info.get("season")
        record_info['season_id'] = SeasonService().get_season_id_by_show_id_and_number(
            show_id=show_id, season_number=season_number
        )

        # Process the episode record
        processed_episode = {
            "maze_id": record_info.get("id"),
            "url": record_info.get("url"),
            "name": record_info.get("name"),
            "number": record_info.get("number"),
            "type": record_info.get("type"),
            "airdate": record_info.get("airdate"),
            "airtime": record_info.get("airtime"),
            "airstamp": record_info.get("airstamp"),
            "runtime": record_info.get("runtime"),
            "image_medium": record_info.get("image", {}).get("medium"),
            "image_original": record_info.get("image", {}).get("original"),
            "summary": record_info.get("summary"),
            "season_id": record_info.get("season_id"),
            "show_id": record_info.get("show_id")
        }

        return processed_episode
