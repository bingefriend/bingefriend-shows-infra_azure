"""Service to manage network-related operations."""
from sqlalchemy.orm import Session

from bingefriend.shows.infra_azure.repositories.network_repo import NetworkRepository


# noinspection PyMethodMayBeStatic
class NetworkService:
    """Service to manage network-related operations."""

    def get_or_create_network(self, network_data, db: Session) -> int | None:
        """Get or create a network entry in the database.

        Args:
            network_data (dict): Data of the network to be created or fetched.
            db (Session): The database session.

        Returns:
            int | None: The primary key of the network if it exists or is created, else None.
        """

        network_repo = NetworkRepository()

        network_id = network_data.get('id')
        if not network_id:
            return None

        # If the network already exists
        network_pk = network_repo.get_network_by_id(network_id, db)
        if network_pk:
            return network_pk

        # If not, create a new network
        network_pk = network_repo.create_network(network_data, db)
        return network_pk
