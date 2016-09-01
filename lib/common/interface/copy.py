class CopyInterface(object):
    """
    Interface to data copy application.
    """

    def __init__(self):
        pass

    def schedule_copy(self, dataset_replica, group, comments = '', auto_approval = True, is_test = False):
        """
        Schedule and execute a copy operation. Argument origin can be None for copy interfaces
        that do not require the origin to be specified.
        Returns the operation id.
        """

        return 0

    def schedule_copies(self, replica_list, group, comments = '', auto_approval = True, is_test = False):
        """
        Schedule mass copies. Subclasses can implement efficient algorithms.
        Returns {operation id: (approved, [replica])}
        """

        request_mapping = {}
        for replica in replica_list:
            operation_id = self.schedule_copy(replica, group, comments, auto_approval, is_test)
            request_mapping[operation_id] = (True, [replica])

        return request_mapping

    def copy_status(self, operation_id):
        """
        Returns the completion status specified by the operation id as a
        {(site, dataset): (last_update, total, copied)} dictionary.
        """

        return {}
