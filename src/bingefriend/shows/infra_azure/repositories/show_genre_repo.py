"""Repository for show-genre data."""

from bingefriend.shows.core.models.show_genre import ShowGenre
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class ShowGenreRepository:
    """Repository for show-genre data."""

    def get_show_genre_by_show_and_genre(self, show_id: int, genre_id: int) -> int | None:
        """Get a show-genre entry by show and genre IDs.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.

        Returns:
            int | None: The primary key of the show-genre entry if it exists, else None.

        """
        db = SessionLocal()

        try:
            show_genre = db.query(ShowGenre).filter(
                ShowGenre.show_id == show_id,
                ShowGenre.genre_id == genre_id
            ).first()
            return show_genre
        finally:
            db.close()

    def create_show_genre(self, show_id: int, genre_id: int) -> None:
        """Create a new show-genre entry in the database.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.

        Returns:
            int | None: The primary key of the created show-genre entry, or None if an error occurred.

        """
        db = SessionLocal()

        try:
            show_genre = ShowGenre(show_id=show_id, genre_id=genre_id)
            db.add(show_genre)
            db.commit()
            db.refresh(show_genre)
        except Exception as e:
            print(f"Error creating show-genre entry: {e}")
            db.rollback()
            db.close()
            return

        db.close()
