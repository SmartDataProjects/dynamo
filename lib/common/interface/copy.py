class CopyInterface(object):
    """
    Interface to data copy application.
    """

    def __init__(self):
        pass

    def schedule_copy(self, dataset_replica, origin = None, comments = ''):
        """
        Schedule and execute a copy operation. Argument origin can be None for copy interfaces
        that do not require the origin to be specified.
        """
        pass

    def schedule_copies(self, replica_origin_list, comments = ''):
        """
        Schedule mass copies. Subclasses can implement efficient algorithms.
        """

        for replica, origin in replica_origin_list:
            self.schedule_copy(replica, origin, comments)
