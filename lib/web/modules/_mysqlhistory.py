from dynamo.utils.interface.mysql import MySQL

class MySQLHistoryMixin(object):
    """
    Mixin to configure a MySQL-based history backend and make the handle available as self.history.
    """

    def __init__(self, config):
        self.history = MySQL(config.history)
