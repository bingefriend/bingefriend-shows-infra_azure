"""Service to manage genre-related operations."""

from bingefriend.shows.infra_azure.repositories.show_genre_repo import ShowGenreRepository


class ShowGenreService:
    """Service to manage show genres."""

    def __init__(self):
        self.show_genre_repo = ShowGenreRepository()

    def create_show_genre(self, show_id: int, genre_id: int) -> int:
        """Get or create a show-genre entry in the database."""

        show_genre_id = self.show_genre_repo.create_show_genre(show_id, genre_id)

        return show_genre_id
