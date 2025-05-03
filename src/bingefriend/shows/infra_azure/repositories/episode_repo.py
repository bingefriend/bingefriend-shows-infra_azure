"""Repository for managing episodes in the database."""
import logging  # Import logging
from typing import Any, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError  # Import SQLAlchemyError
from bingefriend.shows.core.models.episode import Episode


# noinspection PyMethodMayBeStatic
class EpisodeRepository:
    """Repository for managing episodes in the database."""

    def create_episode(self, episode_data: dict[str, Any], db: Session) -> Optional[Episode]:
        """Add a new episode to the database.

        Args:
            episode_data (dict): A dictionary containing episode data.
            db (Session): The database session to use for database operations.

        Returns:
            Optional[Episode]: The created Episode object or None if an error occurred.

        """
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
            return episode
        except SQLAlchemyError as e:  # Catch specific SQLAlchemy errors
            logging.error(f"SQLAlchemyError creating episode entry for maze_id {episode_data.get('id')}: {e}")
            return None
        except Exception as e:  # Catch any other unexpected errors
            logging.error(f"Unexpected error creating episode entry for maze_id {episode_data.get('id')}: {e}")
            return None

    def upsert_episode(self, episode_data: Dict[str, Any], db: Session) -> Optional[int]:
        """Creates a new episode or updates an existing one based on maze_id and show_id.

        Args:
            episode_data (Dict[str, Any]): A dictionary containing episode data from the API.
                                           It's expected that 'show_id' and 'season_id'
                                           (internal DB IDs) have been added to this dict
                                           by the calling service.
            db (Session): The database session to use for database operations.

        Returns:
            Optional[int]: The internal database ID of the created/updated episode,
                           or None if an error occurred or identifiers were missing.
        """
        maze_id = episode_data.get("id")
        show_id = episode_data.get("show_id")  # Internal DB show ID

        episode_id = None  # Initialize episode_id to None

        if not maze_id:
            logging.error("Cannot upsert episode: 'id' (maze_id) is missing from episode_data.")
            return None
        if not show_id:
            # This should ideally not happen if called correctly from EpisodeService
            logging.error(f"Cannot upsert episode maze_id {maze_id}: 'show_id' is missing from episode_data.")
            return None

        # season_id can be None for specials, so we don't strictly require it here,
        # but the calling service should handle logging if it's unexpectedly None.
        season_id = episode_data.get("season_id")  # Internal DB season ID

        try:
            # Attempt to find existing episode by maze_id and show_id
            existing_episode = db.query(Episode).filter(
                Episode.maze_id == maze_id,
                Episode.show_id == show_id
            ).first()

            # Prepare data, handling potential None/empty values and structure
            airdate = episode_data.get('airdate')
            airstamp = episode_data.get('airstamp')
            db_airdate = airdate if airdate else None
            db_airstamp = airstamp if airstamp else None
            image_data = episode_data.get("image") or {}

            # Map API data to Episode model fields
            # Ensure these keys match the attributes of your Episode SQLAlchemy model
            episode_attrs = {
                "maze_id": maze_id,
                "url": episode_data.get("url"),
                "name": episode_data.get("name"),
                "number": episode_data.get("number"),  # API episode number
                "type": episode_data.get("type"),
                "airdate": db_airdate,
                "airtime": episode_data.get("airtime"),
                "airstamp": db_airstamp,
                "runtime": episode_data.get("runtime"),
                "image_medium": image_data.get("medium"),
                "image_original": image_data.get("original"),
                "summary": episode_data.get("summary"),
                "show_id": show_id,  # Foreign key to the Show table
                "season_id": season_id,  # Foreign key to the Season table (can be None)
                # Add other relevant fields from your Episode model here
            }

            if existing_episode:
                # Update existing episode
                logging.debug(
                    f"Updating existing episode with maze_id: {maze_id} for show_id: {show_id} (DB ID: "
                    f"{existing_episode.id})"
                )
                # Filter out None values if you don't want to overwrite existing data with None
                # update_data = {k: v for k, v in episode_attrs.items() if v is not None}
                # Or update all fields regardless:
                update_data = episode_attrs
                for key, value in update_data.items():
                    setattr(existing_episode, key, value)
                episode_id = existing_episode.id
                logging.debug(f"Successfully updated episode maze_id: {maze_id}, internal DB ID: {episode_id}")
            else:
                # Create new episode
                logging.debug(f"Creating new episode with maze_id: {maze_id} for show_id: {show_id}")
                # Filter out keys not present in the model if necessary, or ensure model handles extra keys
                new_episode = Episode(**episode_attrs)
                db.add(new_episode)
                episode_id = new_episode.id
                logging.debug(f"Successfully created episode maze_id: {maze_id}, internal DB ID: {episode_id}")

            return episode_id

        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError upserting episode entry for maze_id {maze_id}, show_id {show_id}: {e}")
            db.rollback()
            return None
        except Exception as e:
            # Catching potential errors during attribute setting or model instantiation
            logging.error(f"Unexpected error upserting episode entry for maze_id {maze_id}, show_id {show_id}: {e}",
                          exc_info=True)
            return None
        finally:
            return episode_id
