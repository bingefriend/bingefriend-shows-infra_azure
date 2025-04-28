"""Repository for show-genre data."""

from bingefriend.shows.core.models.show_genre import ShowGenre
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


class ShowGenreRepository:
    """Repository for show-genre data."""

    def __init__(self):
        self.show_genre_model = ShowGenre

    def get_show_genre_by_show_and_genre(self, show_id: int, genre_id: int) -> int | None:
        """Get a show-genre entry by show and genre IDs."""
        db = SessionLocal()
        try:
            show_genre = db.query(self.show_genre_model).filter(
                self.show_genre_model.show_id == show_id,
                self.show_genre_model.genre_id == genre_id
            ).first()
            return show_genre
        finally:
            db.close()

    def create_show_genre(self, show_id: int, genre_id: int) -> int | None:
        """Create a new show-genre entry in the database."""
        db = SessionLocal()

        try:
            show_genre = self.show_genre_model(show_id=show_id, genre_id=genre_id)
            db.add(show_genre)
            db.commit()
            db.refresh(show_genre)
        except Exception as e:
            print(f"Error creating show-genre entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()

        return show_genre.id if show_genre else None
