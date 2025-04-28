"""Repository for managing shows."""

from typing import Any
from bingefriend.shows.core.models.show import Show
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class ShowRepository:
    """Repository for managing shows."""

    def __init__(self):
        self.show_model = Show

    def create_show(self, show_data: dict[str, Any]) -> int | None:
        """Create a new show entry in the database."""

        db = SessionLocal()
        try:
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
                schedule_time=show_data.get('schedule_time'),
                schedule_days=",".join(show_data.get('schedule_days')),
                network_id=show_data.get('network').get('id'),
                webChannel=show_data.get('webChannel'),
                externals_imdb=show_data.get('externals_imdb'),
                image_medium=show_data.get('image', {}).get('medium'),
                image_original=show_data.get('image', {}).get('original'),
                summary=show_data.get('summary'),
                updated=show_data.get('updated')
            )
            db.add(show)
            db.commit()
            db.refresh(show)
        except Exception as e:
            print(f"Error creating show entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()
        return show.id
