class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self):
        pass

    def schedule_deletion(self, replica):
        """
        Schedule a deletion of the dataset or block replica.
        """

        pass

    def schedule_deletions(self, replica_list):
        """
        Schedule a deletion of multiple replicas. Subclasses should implement the most efficient way
        according to available features.
        """

        for replica in replica_list:
            self.schedule_deletion(replica)
