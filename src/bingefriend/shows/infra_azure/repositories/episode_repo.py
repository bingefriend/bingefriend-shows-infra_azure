"""Repository for managing episodes in the database."""
from typing import Any
from bingefriend.shows.core.models.episode import Episode
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class EpisodeRepository:
    """Repository for managing episodes in the database."""

    def create_episode(self, episode_data: dict[str, Any]) -> Episode | None:
        """Add a new episode to the database.

        Args:
            episode_data (dict): A dictionary containing episode data.

        """
        db = SessionLocal()

        image_data = episode_data.get("image") or {}

        try:
            episode = Episode(
                maze_id=episode_data.get("id"),
                url=episode_data.get("url"),
                name=episode_data.get("name"),
                number=episode_data.get("number"),
                type=episode_data.get("type"),
                airdate=episode_data.get("airdate"),
                airtime=episode_data.get("airtime"),
                airstamp=episode_data.get("airstamp"),
                runtime=episode_data.get("runtime"),
                image_medium=image_data.get("medium"),
                image_original=image_data.get("original"),
                summary=episode_data.get("summary"),
                season_id=episode_data.get("season_id"),
                show_id=episode_data.get("show_id")
            )
            db.add(episode)
            db.commit()
            db.refresh(episode)
        except Exception as e:
            print(f"Error creating episode entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()
        return episode
