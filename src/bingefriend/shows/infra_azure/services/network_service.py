"""Service to manage network-related operations."""
from bingefriend.shows.infra_azure.repositories.network_repo import NetworkRepository


# noinspection PyMethodMayBeStatic
class NetworkService:
    """Service to manage network-related operations."""
    def __init__(self):
        self.network_repo = NetworkRepository()

    def get_or_create_network(self, network_data):
        """Get or create a network entry in the database."""

        network_id = network_data.get('id')
        if not network_id:
            raise ValueError("Network data must contain 'id' field.")

        # Check if the network already exists
        existing_network = self.network_repo.get_network_by_id(network_id)
        if existing_network:
            return existing_network.id

        # Create a new network entry
        new_network = self.network_repo.create_network(network_data)
        return new_network.id
