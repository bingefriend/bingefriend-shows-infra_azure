"""Service to manage network-related operations."""

from bingefriend.shows.infra_azure.repositories.genre_repo import GenreRepository


# noinspection PyMethodMayBeStatic
class GenreService:
    """Service to manage genre-related operations."""
    def __init__(self):
        self.genre_repo = GenreRepository()

    def get_or_create_genre(self, genre_name):
        """Get or create a genre entry in the database."""

        existing_genre_id = self.genre_repo.get_genre_id_by_name(genre_name)

        if existing_genre_id:
            return existing_genre_id

        # Create a new genre entry
        new_genre_id = self.genre_repo.create_genre(genre_name)

        return new_genre_id
