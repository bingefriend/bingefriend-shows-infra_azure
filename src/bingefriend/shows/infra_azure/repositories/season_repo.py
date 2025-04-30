"""Repository for managing seasons in the database."""

from bingefriend.shows.core.models.season import Season
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
