class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self):
        pass

    def schedule_deletion(self, replica):
        """
        Schedule a deletion of the dataset or block replica.
        Return the deletion ID (a number that allows the unique identification of the deletion.)
        """

        return 0

    def schedule_deletions(self, replica_list):
        """
        Schedule a deletion of multiple replicas. Subclasses should implement the most efficient way
        according to available features.
        Returns {operation id: (approved, [replicas])}
        """

        deletion_mapping = {}
        for replica in replica_list:
            deletion_id = self.schedule_deletion(replica)
            deletion_mapping[deletion_id] = (True, [replica])

        return deletion_mapping

    def check_completion(self, operation_id):
        """
        Checks the completion of the deletion specified by the operation id.
        Returns true if completed.
        """

        return False
