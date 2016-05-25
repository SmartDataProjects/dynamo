import time

class AccessHistoryInterface(object):
    """
    Interface to dataset access history data source.
    """

    def __init__(self):
        pass

    def get_local_accesses(self, site, replicas, start_time):
        """
        Fetch the access history for replicas at site starting from start_time and return a list of tuples
        [(dataset, access)]
        """

        return []
