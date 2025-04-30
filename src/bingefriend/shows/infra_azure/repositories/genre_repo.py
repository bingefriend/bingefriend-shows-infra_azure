"""Repository for genre data."""

from bingefriend.shows.core.models.genre import Genre
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class GenreRepository:
    """Repository for genre data."""

    def get_genre_id_by_name(self, name) -> int | None:
        """Get a genre by its name."

        Args:
            name (str): The name of the genre.

        Returns:
            int | None: The ID of the genre if found, otherwise None.

        """

        db = SessionLocal()

        genre = db.query(Genre).filter(Genre.name == name).first()

        if genre:
            return genre.id

        return None

    def create_genre(self, name) -> int | None:
        """Create a new genre entry in the database.

        Args:
            name (str): The name of the genre to be created.

        Returns:
            int | None: The ID of the newly created genre, or None if an error occurred.

        """

        db = SessionLocal()
        try:
            genre = Genre(name=name)
            db.add(genre)
            db.commit()
            db.refresh(genre)
            genre_id = genre.id
        except Exception as e:
            print(f"Error creating genre entry: {e}")
            db.rollback()
            db.close()
            return None

        db.close()

        return genre_id

