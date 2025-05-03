"""Service for managing show-genre relationships."""

import logging
from typing import List

from sqlalchemy.orm import Session

from bingefriend.shows.infra_azure.repositories.show_genre_repo import ShowGenreRepository


class ShowGenreService:
    """Service for managing show-genre relationships."""

    def __init__(self):
        self.repository = ShowGenreRepository()

    def create_show_genre(self, show_id: int, genre_id: int, db: Session) -> None:
        """Creates a link between a show and a genre if it doesn't exist."""
        # This method might be simplified if sync_show_genres is always used for updates
        existing = self.repository.get_show_genre_by_show_and_genre(show_id, genre_id, db)
        if not existing:
            self.repository.create_show_genre(show_id, genre_id, db)
        else:
            logging.debug(f"ShowGenre link already exists for show {show_id}, genre {genre_id}.")

    def sync_show_genres(self, show_id: int, target_genre_ids: List[int], db: Session) -> None:
        """Synchronizes the genre associations for a given show.

        Ensures that the show is linked only to the genres specified in
        target_genre_ids, adding missing links and removing outdated ones.

        Args:
            show_id (int): The internal database ID of the show.
            target_genre_ids (List[int]): A list of internal database genre IDs
                                          that the show should be associated with.
            db (Session): The database session to use for database operations.
        """
        logging.debug(f"Syncing genres for show ID: {show_id}. Target genre IDs: {target_genre_ids}")

        try:
            # 1. Get current genre IDs associated with the show
            current_genre_ids = self.repository.get_genre_ids_for_show(show_id, db)

            # 2. Convert lists to sets for efficient comparison
            current_set = set(current_genre_ids)
            target_set = set(target_genre_ids)

            # 3. Identify genres to add and remove
            genres_to_add = target_set - current_set
            genres_to_remove = current_set - target_set

            # 4. Add new associations
            added_count = 0
            for genre_id in genres_to_add:
                if self.repository.create_show_genre(show_id, genre_id, db):
                    added_count += 1
                else:
                    logging.warning(f"Failed to add genre link for show {show_id}, genre {genre_id}.")

            # 5. Remove outdated associations
            removed_count = 0
            for genre_id in genres_to_remove:
                if self.repository.delete_show_genre(show_id, genre_id, db):
                    removed_count += 1
                else:
                    logging.warning(f"Failed to remove genre link for show {show_id}, genre {genre_id}.")

            logging.info(f"Genre sync for show ID {show_id} complete. Added: {added_count}, Removed: {removed_count}.")

        except Exception as e:
            # Catch potential errors during the process
            logging.error(f"Unexpected error during genre sync for show ID {show_id}: {e}", exc_info=True)
