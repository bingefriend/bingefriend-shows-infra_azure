"""Repository for managing shows."""

from typing import Any
from bingefriend.shows.core.models.show import Show
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class ShowRepository:
    """Repository for managing shows."""

    def create_show(self, show_data: dict[str, Any]) -> int | None:
        """Create a new show entry in the database.

        Args:
            show_data (dict): Data of the show to be created.

        Returns:
            int | None: The primary key of the created show entry, or None if an error occurred.

        """

        db = SessionLocal()
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
            db.commit()
            db.refresh(show)
            show_id = show.id
        except Exception as e:
            print(f"Error creating show entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()

        return show_id
