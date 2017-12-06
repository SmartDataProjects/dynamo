class CopyInterface(object):
    """
    Interface to data copy application.
    """

    def __init__(self, config):
        pass

    def schedule_copy(self, replica, comments = ''):
        """
        Schedule and execute a copy operation.
        @param replica  DatasetReplica or BlockReplica
        @param comments Comments to be passed to the external interface.
        @return {operation_id: (approved, site, [dataset/block])}
        """

        raise NotImplementedError('schedule_copy')

    def schedule_copies(self, replica_list, comments = ''):
        """
        Schedule mass copies. Subclasses can implement efficient algorithms.
        @param replica_list  List of DatasetReplicas and BlockReplicas
        @param comments      Comments to be passed to the external interface.
        @return {operation_id: (approved, site, [dataset/block])}
        """

        request_mapping = {}
        for replica in replica_list:
            request_mapping.update(self.schedule_copy(replica, comments))

        return request_mapping

    def copy_status(self, operation_id):
        """
        Returns the completion status specified by the operation id as a
        {(site, dataset): (last_update, total, copied)} dictionary.
        """

        raise NotImplementedError('copy_status')
