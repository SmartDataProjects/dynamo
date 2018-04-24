from dynamo.registry.registry import DynamoRegistry
from dynamo.utils.interface.mysql import MySQL

class MySQLRegistry(DynamoRegistry):
    """Registry with MySQL backend."""
    def __init__(self, config):
        self.backend = MySQL(config.db_params)
