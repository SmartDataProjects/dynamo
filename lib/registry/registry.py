from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class RegistryDatabase(object):
    """
    Similar to HistoryDatabase, this is just one abstraction layer that doesn't really hide the
    backend technology for the registry. We still have the benefit of being able to use default
    parameters to initialize the registry database handle.
    """

        # default configuration
    _config = Configuration()

    @staticmethod
    def set_default(config):
        RegistryDatabase._config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = RegistryDatabase._config

        self.db = MySQL(config.db_params)

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = True
