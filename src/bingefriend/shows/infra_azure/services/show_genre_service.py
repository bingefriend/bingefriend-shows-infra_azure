"""Service to manage genre-related operations."""

import logging
from bingefriend.shows.infra_azure.repositories.show_genre_repo import ShowGenreRepository


# noinspection PyMethodMayBeStatic
class ShowGenreService:
    """Service to manage show genres."""

    def create_show_genre(self, show_id: int, genre_id: int) -> None:
        """Get or create a show-genre entry in the database.

        Args:
            show_id (int): The ID of the show.
            genre_id (int): The ID of the genre.

        Returns:
            None: No return value.

        """

        show_genre_repo = ShowGenreRepository()
        show_genre_repo.create_show_genre(show_id, genre_id)
        logging.info(f"Created show-genre association: show_id={show_id}, genre_id={genre_id}")

        return
