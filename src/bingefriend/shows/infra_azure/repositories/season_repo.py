"""Repository for managing seasons in the database."""

from bingefriend.shows.core.models.season import Season
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


class SeasonRepository:
    """Repository to handle season-related database operations."""

    def __init__(self):
        self.seaon_model = Season

    def get_season_id_by_show_id_and_number(self, show_id: int, season_number: int) -> int | None:
        """Get the season ID for a given show ID and season number."""

        db = SessionLocal()

        season = db.query(self.seaon_model).filter(
            self.seaon_model.show_id == show_id,
            self.seaon_model.number == season_number
        ).first()

        if not season:
            return None

        return season.id
