"""Repository for web channel data."""

from bingefriend.shows.core.models.web_channel import WebChannel
from sqlalchemy.orm import Session


# noinspection PyMethodMayBeStatic
class WebChannelRepository:
    """Repository to interact with web channel."""

    def get_web_channel_by_maze_id(self, maze_id: int, db: Session) -> int | None:
        """Get a web channel by its TV Maze ID.

        Args:
            maze_id (int): The ID of the web channel in TV Maze.
            db (Session): The database session.

        Returns:
            int | None: The primary key of the web channel if it exists, else None.

        """
        try:
            web_channel: WebChannel | None = db.query(
                WebChannel
            ).filter(
                WebChannel.maze_id == maze_id
            ).first()
        except Exception as e:
            print(f"Error fetching web channel by ID: {e}")
            return None

        if not web_channel:
            return None

        web_channel_pk = web_channel.id

        return web_channel_pk

    def create_web_channel(self, web_channel_data, db: Session) -> int | None:
        """Create a new web channel entry in the database.

        Args:
            web_channel_data (dict): Data of the web channel to be created.
            db (Session): The database session.

        Returns:
            int | None: The primary key of the created web channel entry, or None if an error occurred.

        """
        try:
            country_data = web_channel_data.get('country') or {}

            new_web_channel = WebChannel(
                maze_id=web_channel_data.get('id'),
                name=web_channel_data.get('name'),
                country_name=country_data.get('name'),
                country_timezone=country_data.get('timezone'),
                country_code=country_data.get('code'),
                official_site=web_channel_data.get('officialSite'),
            )
            db.add(new_web_channel)
            web_channel_pk = new_web_channel.id
        except Exception as e:
            print(f"Error creating web channel entry: {e}")
            return None

        return web_channel_pk
