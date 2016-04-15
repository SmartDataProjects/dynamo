import time

class AccessHistoryInterface(object):
    """
    Interface to dataset access history data source.
    """

    def __init__(self):
        pass

    def set_access_history(self, site, replicas, start_time):
        """
        Fetch the access history for replicas at site starting from start_time and set replica.accesses
        """
        pass
