"""Repository for network data."""

from bingefriend.shows.core.models.network import Network
from bingefriend.shows.infra_azure.repositories.database import SessionLocal


# noinspection PyMethodMayBeStatic
class NetworkRepository:
    """Repository for network data."""

    def __init__(self):
        self.network_model = Network

    def get_network_by_id(self, network_id):
        """Get a network by its ID."""

        db = SessionLocal()
        try:
            network = db.query(self.network_model).filter(self.network_model.maze_id == network_id).first()
            return network
        finally:
            db.close()

    def create_network(self, network_data):
        """Create a new network entry in the database."""

        db = SessionLocal()
        try:
            network = self.network_model(
                maze_id=network_data.get('id'),
                name=network_data.get('name'),
                country_name=network_data.get('country', {}).get('name'),
                country_timezone=network_data.get('country', {}).get('timezone'),
                country_code=network_data.get('country', {}).get('code')
            )
            db.add(network)
            db.commit()
            db.refresh(network)
            return network
        finally:
            db.close()
