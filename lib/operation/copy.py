from dynamo.dataformat import Configuration
from dynamo.utils.classutil import get_instance

class CopyInterface(object):
    """
    Interface to data copy application.
    """

    @staticmethod
    def get_instance(module = None, config = None):
        if module is None:
            module = CopyInterface._module
        if config is None:
            config = CopyInterface._config

        return get_instance(CopyInterface, module, config)

    _module = ''
    _config = Configuration()

    @staticmethod
    def set_default(config):
        CopyInterface._module = config.module
        CopyInterface._config = config.config

    def __init__(self, config = None):
        config = Configuration(config)

        self.dry_run = config.get('dry_run', False)
        self._next_operation_id = 1

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
        {(site_name, item_name): (total_bytes, copied_bytes, last_update)} dictionary.
        """

        raise NotImplementedError('copy_status')
