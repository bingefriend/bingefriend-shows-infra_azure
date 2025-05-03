"""Repository for managing shows."""
import logging
from typing import Any, Optional
from bingefriend.shows.core.models.show import Show
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


# noinspection PyMethodMayBeStatic
class ShowRepository:
    """Repository for managing shows."""

    def create_show(self, show_data: dict[str, Any], db: Session) -> int | None:
        """Create a new show entry in the database.

        Args:
            show_data (dict): Data of the show to be created.
            db (Session): The database session to use for the operation.

        Returns:
            int | None: The primary key of the created show entry, or None if an error occurred.

        """
        try:
            schedule_data = show_data.get('schedule') or {}
            image_data = show_data.get('image') or {}
            externals_data = show_data.get('externals') or {}

            show = Show(
                maze_id=show_data.get('id'),
                url=show_data.get('url'),
                name=show_data.get('name'),
                type=show_data.get('type'),
                language=show_data.get('language'),
                status=show_data.get('status'),
                runtime=show_data.get('runtime'),
                averageRuntime=show_data.get('averageRuntime'),
                premiered=show_data.get('premiered'),
                ended=show_data.get('ended'),
                schedule_time=schedule_data.get('time'),
                schedule_days=",".join(schedule_data.get('days')),
                network_id=show_data.get('network_id'),
                webChannel_id=show_data.get('webChannel_id'),
                externals_imdb=externals_data.get('imdb'),
                image_medium=image_data.get('medium'),
                image_original=image_data.get('original'),
                summary=show_data.get('summary'),
                updated=show_data.get('updated')
            )
            db.add(show)
            show_id = show.id
        except Exception as e:
            print(f"Error creating show entry: {e}")
            return None

        return show_id

    def upsert_show(self, show_data: dict[str, Any], db: Session) -> Optional[int]:
        """Creates a new show or updates an existing one based on maze_id.

        Args:
            show_data (dict[str, Any]): A dictionary containing show data from the API.
                                        It's expected that 'network_id' and 'web_channel_id'
                                        have already been resolved and added to this dict
                                        by the calling service.
            db (Session): The database session to use for the operation.

        Returns:
            Optional[int]: The internal database ID of the created/updated show,
                           or None if an error occurred or maze_id was missing.
        """
        maze_id = show_data.get("id")

        if not maze_id:
            logging.error("Cannot upsert show: 'id' (maze_id) is missing from show_data.")
            return None

        show_id = None  # Initialize show_id to None

        try:
            # Attempt to find existing show by maze_id
            existing_show = db.query(Show).filter(Show.maze_id == maze_id).first()

            # Prepare data, handling potential None/empty values and structure
            premiered_date = show_data.get('premiered')
            ended_date = show_data.get('ended')
            db_premiered = premiered_date if premiered_date else None
            db_ended = ended_date if ended_date else None

            image_data = show_data.get("image") or {}
            schedule_data = show_data.get("schedule") or {}
            externals_data = show_data.get("externals") or {}
            # Convert schedule days list to comma-separated string for storage
            schedule_days_list = schedule_data.get("days", [])
            schedule_days_str = ",".join(schedule_days_list) if schedule_days_list else None

            # Map API data to Show model fields
            # Ensure these keys match the attributes of your Show SQLAlchemy model
            show_attrs = {
                "maze_id": maze_id,
                "url": show_data.get("url"),
                "name": show_data.get("name"),
                "type": show_data.get("type"),
                "language": show_data.get("language"),
                "status": show_data.get("status"),
                "runtime": show_data.get("runtime"),
                "averageRuntime": show_data.get("averageRuntime"),
                "premiered": db_premiered,
                "ended": db_ended,
                "schedule_time": schedule_data.get("time"),
                "schedule_days": schedule_days_str,
                "network_id": show_data.get("network_id"),  # Provided by ShowService
                "webChannel_id": show_data.get("web_channel_id"),  # Provided by ShowService
                "externals_imdb": externals_data.get("imdb"),
                "image_medium": image_data.get("medium"),
                "image_original": image_data.get("original"),
                "summary": show_data.get("summary"),
                "updated": show_data.get("updated"),  # API's last update timestamp
            }

            if existing_show:
                # Update existing show
                logging.debug(f"Updating existing show with maze_id: {maze_id} (DB ID: {existing_show.id})")
                # Filter out None values if you don't want to overwrite existing data with None
                # update_data = {k: v for k, v in show_attrs.items() if v is not None}
                # Or update all fields regardless:
                update_data = show_attrs
                for key, value in update_data.items():
                    setattr(existing_show, key, value)
                show_id = existing_show.id
                logging.info(f"Successfully updated show maze_id: {maze_id}, internal DB ID: {show_id}")
            else:
                # Create new show
                logging.debug(f"Creating new show with maze_id: {maze_id}")
                # Filter out keys not present in the model if necessary, or ensure model handles extra keys
                new_show = Show(**show_attrs)
                db.add(new_show)
                show_id = new_show.id
                logging.info(f"Successfully created show maze_id: {maze_id}, internal DB ID: {show_id}")

            return show_id

        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError upserting show entry for maze_id {maze_id}: {e}")
            return None
        except Exception as e:
            # Catching potential errors during attribute setting or model instantiation
            logging.error(f"Unexpected error upserting show entry for maze_id {maze_id}: {e}", exc_info=True)
            return None
        finally:
            return show_id
