class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self, config):
        self.dry_run = config.get('dry_run', False)

    def schedule_deletion(self, replica, comments = ''):
        """
        Schedule a deletion of the dataset or block replica.
        @param replica   A DatasetReplica or BlockReplica
        @param comments  Comments to be passed to the operation interface
        @return {operation id, approved, site, [dataset/block]}
        """

        raise NotImplementedError('schedule_deletion')

    def schedule_deletions(self, replica_list, comments = ''):
        """
        Schedule a deletion of multiple replicas. Subclasses should implement the most efficient way
        according to available features.
        @param replica_list  A flat list of DatasetReplicas or BlockReplicas
        @param comments      Comments to be pased to the operation interface
        @return {operation id: (approved, site, [dataset/block])}
        """

        request_mapping = {}
        for replica in replica_list:
            request_mapping.update(self.schedule_deletion(replica, comments = comments))

        return request_mapping

    def deletion_status(self, operation_id):
        """
        @param operation_id  Operation id returned by schedule_deletion.
        @return Completion status {dataset: (last_update, total, deleted)}
        """

        raise NotImplementedError('deletion_status')
