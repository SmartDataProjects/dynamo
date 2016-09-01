class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self):
        pass

    def schedule_deletion(self, replica, comments = '', auto_approval = True, is_test = False):
        """
        Schedule a deletion of the dataset or block replica.
        Returns (operation id, approved, [replicas])
        """

        return None

    def schedule_deletions(self, replica_list, comments = '', auto_approval = True, is_test = False):
        """
        Schedule a deletion of multiple replicas. Subclasses should implement the most efficient way
        according to available features.
        Returns {operation id: (approved, [replicas])}
        """

        deletion_mapping = {}
        for replica in replica_list:
            result = self.schedule_deletion(replica, comments = comments, auto_approval = auto_approval, is_test = is_test)
            if result is None:
                continue

            deletion_id, approved, replicas = result
            if deletion_id != 0:
                deletion_mapping[deletion_id] = (approved, replicas)

        return deletion_mapping

    def deletion_status(self, operation_id):
        """
        Returns the completion status specified by the operation id as a
        {dataset: (last_update, total, deleted)} dictionary.
        """

        return {}
