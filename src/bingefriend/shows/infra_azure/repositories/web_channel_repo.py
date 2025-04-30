"""Repository for web channel data."""

from bingefriend.shows.core.models.web_channel import WebChannel
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class WebChannelRepository:
    """Repository to interact with web channel."""

    def get_web_channel_by_maze_id(self, maze_id) -> int | None:
        """Get a web channel by its TV Maze ID.

        Args:
            maze_id (int): The ID of the web channel in TV Maze.

        Returns:
            int | None: The primary key of the web channel if it exists, else None.

        """

        db = SessionLocal()

        try:
            web_channel = db.query(
                WebChannel
            ).filter(
                WebChannel.maze_id == maze_id
            ).first()
        except Exception as e:
            print(f"Error fetching web channel by ID: {e}")
            db.close()
            return None

        if not web_channel:
            db.close()
            return None

        db.close()

        web_channel_pk = web_channel.id

        return web_channel_pk

    def create_web_channel(self, web_channel_data) -> int | None:
        """Create a new web channel entry in the database.

        Args:
            web_channel_data (dict): Data of the web channel to be created.

        Returns:
            int | None: The primary key of the created web channel entry, or None if an error occurred.

        """

        db = SessionLocal()

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
            db.commit()
            db.refresh(new_web_channel)
            web_channel_pk = new_web_channel.id
        except Exception as e:
            print(f"Error creating web channel entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()

        return web_channel_pk
