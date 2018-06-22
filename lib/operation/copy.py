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
        self._read_only = False

    def set_read_only(self, value = True):
        self._read_only = value

    def schedule_copies(self, replica_list, operation_id, comments = ''):
        """
        Schedule copies.
        @param replica_list  List of DatasetReplicas. Must be replicas at a single site.
        @param operation_id  Copy operation id in the history DB for logging.
        @param comments      Comments to be passed to the external interface.
        @return List of successfully scheduled replicas (cloned objects from replica_list)
        """
        raise NotImplementedError('schedule_copies')

    def copy_status(self, operation_id):
        """
        Returns the completion status specified by the operation id as a
        {(site_name, item_name): (total_bytes, copied_bytes, last_update)} dictionary.
        """

        raise NotImplementedError('copy_status')
