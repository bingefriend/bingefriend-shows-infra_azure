"""Service to manage network-related operations."""
from sqlalchemy.orm import Session

from bingefriend.shows.infra_azure.repositories.genre_repo import GenreRepository


# noinspection PyMethodMayBeStatic
class GenreService:
    """Service to manage genre-related operations."""

    def get_or_create_genre(self, genre_name: str, db: Session):
        """Get or create a genre entry in the database.
        Args:
            genre_name (str): Name of the genre to be created or fetched.
            db (Session): SQLAlchemy session object.

        Returns:
            int: The primary key of the genre if it exists or is created.

        """

        genre_repo = GenreRepository()
        existing_genre_id = genre_repo.get_genre_id_by_name(genre_name, db)

        if existing_genre_id:
            return existing_genre_id

        # Create a new genre entry
        new_genre_id = genre_repo.create_genre(genre_name, db)

        return new_genre_id
