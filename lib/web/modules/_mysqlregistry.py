from dynamo.utils.interface.mysql import MySQL

class MySQLRegistryMixin(object):
    """
    Mixin to configure a MySQL-based registry backend and make the handle available as self.registry.
    """

    def __init__(self, config):
        self.registry = MySQL(config.registry)
