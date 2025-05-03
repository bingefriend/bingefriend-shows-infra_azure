"""Repository for genre data."""

from bingefriend.shows.core.models.genre import Genre
from sqlalchemy.orm import Session


# noinspection PyMethodMayBeStatic
class GenreRepository:
    """Repository for genre data."""

    def get_genre_id_by_name(self, name: str, db: Session) -> int | None:
        """Get a genre by its name."

        Args:
            name (str): The name of the genre.
            db (Session): SQLAlchemy session object.

        Returns:
            int | None: The ID of the genre if found, otherwise None.

        """

        genre: Genre | None = db.query(Genre).filter(Genre.name == name).first()

        if genre:
            return genre.id

        return None

    def create_genre(self, name: str, db: Session) -> int | None:
        """Create a new genre entry in the database.

        Args:
            name (str): The name of the genre to be created.
            db (Session): SQLAlchemy session object.

        Returns:
            int | None: The ID of the newly created genre, or None if an error occurred.

        """

        try:
            genre = Genre(name=name)
            db.add(genre)
            genre_id = genre.id
        except Exception as e:
            print(f"Error creating genre entry: {e}")
            return None

        return genre_id

