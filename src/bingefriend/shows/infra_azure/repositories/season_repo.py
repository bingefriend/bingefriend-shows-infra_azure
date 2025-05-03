"""Repository for managing seasons in the database."""
import logging
from typing import Any, Optional

from bingefriend.shows.core.models.season import Season
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class SeasonRepository:
    """Repository to handle season-related database operations."""

    def create_season(self, season_data: dict) -> Season | None:
        """Create a new season entry in the database.

        Args:
            season_data (dict): A dictionary containing season data.

        Returns:
            Season: The created season object.

        """
        db = SessionLocal()

        image_data = season_data.get("image") or {}

        try:
            season = Season(
                maze_id=season_data.get("id"),
                url=season_data.get("url"),
                number=season_data.get("number"),
                name=season_data.get("name"),
                episodeOrder=season_data.get("episodeOrder"),
                premiereDate=season_data.get("premiereDate"),
                endDate=season_data.get("endDate"),
                network_id=season_data.get("network_id"),
                image_medium=image_data.get("medium"),
                image_original=image_data.get("original"),
                summary=season_data.get("summary"),
                show_id=season_data.get("show_id")
            )
            db.add(season)
            db.commit()
            db.refresh(season)
        except Exception as e:
            print(f"Error creating season entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()
        return season

    def get_season_id_by_show_id_and_number(self, show_id: int, season_number: int) -> int | None:
        """Get the season ID for a given show ID and season number.

        Args:
            show_id (int): The ID of the show.
            season_number (int): The season number.

        """

        db = SessionLocal()

        try:
            season = db.query(Season).filter(
                Season.show_id == show_id,
                Season.number == season_number
            ).first()
        except Exception as e:
            print(f"Error fetching season ID: {e}")
            db.rollback()
            db.close()
            return None

        db.close()

        if not season:
            return None

        return season.id

    def upsert_season(self, season_data: dict[str, Any]) -> Optional[int]:
        """Creates a new season or updates an existing one based on maze_id and show_id.

        Args:
            season_data (dict[str, Any]): A dictionary containing season data from the API.
                                          It's expected that 'show_id' (internal DB ID)
                                          and potentially 'network_id' have been added
                                          to this dict by the calling service.

        Returns:
            Optional[int]: The internal database ID of the created/updated season,
                           or None if an error occurred or identifiers were missing.
        """
        db: Session = SessionLocal()
        maze_id = season_data.get("id")
        show_id = season_data.get("show_id")  # Internal DB show ID

        season_id = None  # Initialize season_id to None

        if not maze_id:
            logging.error("Cannot upsert season: 'id' (maze_id) is missing from season_data.")
            db.close()
            return None
        if not show_id:
            # This should ideally not happen if called correctly from SeasonService
            logging.error(f"Cannot upsert season maze_id {maze_id}: 'show_id' is missing from season_data.")
            db.close()
            return None

        try:
            # Attempt to find existing season by maze_id and show_id
            existing_season = db.query(Season).filter(
                Season.maze_id == maze_id,
                Season.show_id == show_id
            ).first()

            # Prepare data, handling potential None/empty values and structure
            premiere_date = season_data.get('premiereDate')
            end_date = season_data.get('endDate')
            db_premiere = premiere_date if premiere_date else None
            db_end = end_date if end_date else None
            image_data = season_data.get("image") or {}

            # Map API data to Season model fields
            # Ensure these keys match the attributes of your Season SQLAlchemy model
            season_attrs = {
                "maze_id": maze_id,
                "show_id": show_id,  # Foreign key to the Show table
                "url": season_data.get("url"),
                "number": season_data.get("number"),
                "name": season_data.get("name"),
                "episode_order": season_data.get("episodeOrder"),  # Verify model field name
                "premiere_date": db_premiere,  # Verify model field name
                "end_date": db_end,  # Verify model field name
                "network_id": season_data.get("network_id"),  # Provided by SeasonService if available
                # Note: webChannel is often null for seasons, handle if needed
                # "web_channel_id": season_data.get("web_channel_id"), # Needs resolving like in ShowService if used
                "image_medium": image_data.get("medium"),
                "image_original": image_data.get("original"),
                "summary": season_data.get("summary"),
                # Add other relevant fields from your Season model here
            }

            if existing_season:
                # Update existing season
                logging.debug(
                    f"Updating existing season with maze_id: {maze_id} for show_id: {show_id} (DB ID: "
                    f"{existing_season.id})"
                )
                # Filter out None values if you don't want to overwrite existing data with None
                # update_data = {k: v for k, v in season_attrs.items() if v is not None}
                # Or update all fields regardless:
                update_data = season_attrs
                for key, value in update_data.items():
                    setattr(existing_season, key, value)
                db.commit()
                db.refresh(existing_season)  # Ensure the object reflects committed state
                season_id = existing_season.id
                logging.debug(f"Successfully updated season maze_id: {maze_id}, internal DB ID: {season_id}")
            else:
                # Create new season
                logging.debug(f"Creating new season with maze_id: {maze_id} for show_id: {show_id}")
                # Filter out keys not present in the model if necessary, or ensure model handles extra keys
                new_season = Season(**season_attrs)
                db.add(new_season)
                db.commit()
                db.refresh(new_season)  # Get the generated ID and reflect committed state
                season_id = new_season.id
                logging.debug(f"Successfully created season maze_id: {maze_id}, internal DB ID: {season_id}")

            return season_id

        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError upserting season entry for maze_id {maze_id}, show_id {show_id}: {e}")
            db.rollback()
            return None
        except Exception as e:
            # Catching potential errors during attribute setting or model instantiation
            logging.error(f"Unexpected error upserting season entry for maze_id {maze_id}, show_id {show_id}: {e}",
                          exc_info=True)
            db.rollback()
            return None
        finally:
            db.close()
            return season_id
