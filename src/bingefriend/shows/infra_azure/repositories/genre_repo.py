"""Repository for genre data."""

from bingefriend.shows.core.models.genre import Genre
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


class GenreRepository:
    """Repository for genre data."""

    def __init__(self):
        self.genre_model = Genre

    def get_genre_id_by_name(self, name) -> int | None:
        """Get a genre by its name."""

        db = SessionLocal()

        genre = db.query(self.genre_model).filter(self.genre_model.name == name).first()

        if genre:
            return genre.id

        return None

    def create_genre(self, name):
        """Create a new genre entry in the database."""

        db = SessionLocal()

        genre = self.genre_model(name=name)

        db.add(genre)
        db.commit()
        db.refresh(genre)

        return genre.id

