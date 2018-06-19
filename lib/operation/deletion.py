from dynamo.dataformat import Configuration
from dynamo.utils.classutil import get_instance

class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    @staticmethod
    def get_instance(module = None, config = None):
        if module is None:
            module = DeletionInterface._module
        if config is None:
            config = DeletionInterface._config

        return get_instance(DeletionInterface, module, config)

    _module = ''
    _config = Configuration()

    @staticmethod
    def set_default(config):
        DeletionInterface._module = config.module
        DeletionInterface._config = config.config

    def __init__(self, config = None):
        config = Configuration(config)

        self.dry_run = config.get('dry_run', False)
        self._next_operation_id = 1

    def schedule_deletions(self, replica_list, operation_id, comments = ''):
        """
        Schedule a deletion of multiple replicas.
        @param replica_list  [(DatasetReplica, [BlockReplica])]. List of block replicas can be None if deleting the entire dataset replica.
        @param operation_id  Deletion operation id in the history DB for logging.
        @param comments      Comments to be pased to the operation interface

        @return  Clone [(DatasetReplica, [BlockReplica] or None)] for successfully scheduled replicas
        """

        raise NotImplementedError('schedule_deletions')

    def deletion_status(self, operation_id):
        """
        @param operation_id  Operation id returned by schedule_deletion.
        @return Completion status {dataset: (last_update, total, deleted)}
        """

        raise NotImplementedError('deletion_status')
