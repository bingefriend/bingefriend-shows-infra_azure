"""Repository for show-genre data."""

import logging
from typing import Optional, List, Any
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from bingefriend.shows.core.models.show_genre import ShowGenre


# noinspection PyMethodMayBeStatic,PyUnusedLocal
class ShowGenreRepository:
    """Repository for show-genre data."""

    def get_show_genre_by_show_and_genre(self, show_id: int, genre_id: int, db: Session) -> Optional[ShowGenre]:
        """Get a show-genre entry by show and genre IDs.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.
            db (Session): The database session.

        Returns:
            Optional[ShowGenre]: The ShowGenre object if found, else None.
        """
        try:
            show_genre = db.query(ShowGenre).filter(
                ShowGenre.show_id == show_id,
                ShowGenre.genre_id == genre_id
            ).first()
            return show_genre
        except SQLAlchemyError as e:
            logging.error(f"Error fetching show_genre for show {show_id}, genre {genre_id}: {e}")
            return None

    def get_genre_ids_for_show(self, show_id: int, db: Session) -> Any:
        """Get all genre IDs associated with a specific show ID.

        Args:
            show_id (int): The ID of the show.
            db (Session): The database session.

        Returns:
            List[int]: A list of genre IDs associated with the show.
        """
        genre_ids = []

        try:
            results = db.query(ShowGenre.genre_id).filter(ShowGenre.show_id == show_id).all()
            genre_ids = [result[0] for result in results]
            return genre_ids
        except SQLAlchemyError as e:
            logging.error(f"Error fetching genre IDs for show {show_id}: {e}")
            return []  # Return empty list on error

    def create_show_genre(self, show_id: int, genre_id: int, db: Session) -> bool | None:
        """Create a new show-genre entry in the database.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.
            db (Session): The database session.

        Returns:
            bool: True if creation was successful, False otherwise.
        """
        # Avoid creating duplicates if it already exists
        if self.get_show_genre_by_show_and_genre(show_id, genre_id, db):
            logging.debug(f"ShowGenre link already exists for show {show_id}, genre {genre_id}. Skipping creation.")
            return True  # Treat as success if already exists

        try:
            show_genre = ShowGenre(show_id=show_id, genre_id=genre_id)
            db.add(show_genre)
            logging.debug(f"Created ShowGenre link for show {show_id}, genre {genre_id}.")
            return True
        except SQLAlchemyError as e:
            logging.error(f"Error creating show-genre entry for show {show_id}, genre {genre_id}: {e}")
            return False

    def delete_show_genre(self, show_id: int, genre_id: int, db: Session) -> bool | None:
        """Delete a show-genre entry from the database.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.
            db (Session): The database session.

        Returns:
            bool: True if deletion was successful or entry didn't exist, False otherwise.
        """
        try:
            show_genre = db.query(ShowGenre).filter(
                ShowGenre.show_id == show_id,
                ShowGenre.genre_id == genre_id
            ).first()

            if show_genre:
                db.delete(show_genre)
                logging.debug(f"Deleted ShowGenre link for show {show_id}, genre {genre_id}.")
            else:
                logging.debug(f"ShowGenre link not found for show {show_id}, genre {genre_id}. Skipping deletion.")
            return True
        except SQLAlchemyError as e:
            logging.error(f"Error deleting show-genre entry for show {show_id}, genre {genre_id}: {e}")
            return False
