"""Repository for managing episodes in the database."""
import logging  # Import logging
from typing import Any, Optional
from bingefriend.shows.core.models.episode import Episode
from bingefriend.shows.infra_azure.repositories.database import SessionLocal
from sqlalchemy.exc import SQLAlchemyError  # Import SQLAlchemyError


# noinspection PyMethodMayBeStatic
class EpisodeRepository:
    """Repository for managing episodes in the database."""

    def create_episode(self, episode_data: dict[str, Any]) -> Optional[Episode]:
        """Add a new episode to the database.

        Args:
            episode_data (dict): A dictionary containing episode data.

        Returns:
            Optional[Episode]: The created Episode object or None if an error occurred.

        """
        db = SessionLocal()

        image_data = episode_data.get("image") or {}

        # --- Handle potentially empty date/time strings ---
        airdate = episode_data.get('airdate')
        airtime = episode_data.get('airtime')
        airstamp = episode_data.get('airstamp')

        # Convert empty strings to None for database insertion
        db_airdate = airdate if airdate else None
        db_airtime = airtime if airtime else None
        db_airstamp = airstamp if airstamp else None
        # --- End handling ---

        try:
            episode = Episode(
                maze_id=episode_data.get("id"),
                url=episode_data.get("url"),
                name=episode_data.get("name"),
                number=episode_data.get("number"),
                type=episode_data.get("type"),
                airdate=db_airdate,  # Use processed value
                airtime=db_airtime,  # Use processed value
                airstamp=db_airstamp,  # Use processed value
                runtime=episode_data.get("runtime"),
                image_medium=image_data.get("medium"),
                image_original=image_data.get("original"),
                summary=episode_data.get("summary"),
                season_id=episode_data.get("season_id"),  # Ensure this is passed correctly
                show_id=episode_data.get("show_id")  # Ensure this is passed correctly
            )
            db.add(episode)
            db.commit()
            db.refresh(episode)
            # Optional: Add success logging if desired
            # logging.info(f"Successfully created episode {episode.id} (Maze ID: {episode.maze_id})")
            return episode
        except SQLAlchemyError as e:  # Catch specific SQLAlchemy errors
            logging.error(f"SQLAlchemyError creating episode entry for maze_id {episode_data.get('id')}: {e}")
            db.rollback()
            return None
        except Exception as e:  # Catch any other unexpected errors
            logging.error(f"Unexpected error creating episode entry for maze_id {episode_data.get('id')}: {e}")
            db.rollback()
            return None
        finally:
            db.close()
