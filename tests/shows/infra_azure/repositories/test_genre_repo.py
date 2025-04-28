"""Unit tests for GenreRepository class."""

from unittest.mock import patch, MagicMock

from dotenv import load_dotenv
load_dotenv('tests/.env.test')  # Load environment variables for testing

from bingefriend.shows.core.models.genre import Genre
from bingefriend.shows.infra_azure.repositories import database
from bingefriend.shows.infra_azure.repositories.genre_repo import GenreRepository
from bingefriend.shows.core.models.show_genre import ShowGenre
from bingefriend.shows.core.models.show import Show
from bingefriend.shows.core.models.season import Season
from bingefriend.shows.core.models.episode import Episode
from bingefriend.shows.core.models.network import Network


# Test class for GenreRepository
class TestGenreRepository:
    """Test class for GenreRepository."""
    @patch('bingefriend.shows.infra_azure.repositories.genre_repo.SessionLocal')
    def test_get_genre_id_by_name_exists(self, mock_session_local):
        """Test get_genre_id_by_name when the genre exists."""
        # Arrange
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_genre = MagicMock(spec=Genre)
        mock_genre.id = 123

        mock_session_local.return_value = mock_session
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.first.return_value = mock_genre

        repo = GenreRepository()
        genre_name = "Action"

        # Act
        genre_id = repo.get_genre_id_by_name(genre_name)

        # Assert
        mock_session_local.assert_called_once()
        mock_session.query.assert_called_once_with(repo.genre_model)
        # Cannot easily assert filter condition without more complex mocking,
        # but we check that filter and first were called.
        mock_query.filter.assert_called_once()
        mock_filter.first.assert_called_once()
        assert genre_id == 123

    @patch('bingefriend.shows.infra_azure.repositories.genre_repo.SessionLocal')
    def test_get_genre_id_by_name_not_exists(self, mock_session_local):
        """Test get_genre_id_by_name when the genre does not exist."""
        # Arrange
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_filter = MagicMock()

        mock_session_local.return_value = mock_session
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.first.return_value = None  # Simulate genre not found

        repo = GenreRepository()
        genre_name = "NonExistentGenre"

        # Act
        genre_id = repo.get_genre_id_by_name(genre_name)

        # Assert
        mock_session_local.assert_called_once()
        mock_session.query.assert_called_once_with(repo.genre_model)
        mock_query.filter.assert_called_once()
        mock_filter.first.assert_called_once()
        assert genre_id is None
