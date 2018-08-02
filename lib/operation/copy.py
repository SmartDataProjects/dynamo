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

    def copy_status(self, history_record, inventory): #override
        try:
            site = inventory.sites[history_record.site_name]
        except KeyError:
            site = None

        result = {}
        for replica in history_record.replicas:
            key = (history_record.site_name, replica.dataset_name)

            if site is None:
                result[key] = (replica.size, 0, history_record.timestamp)
                continue

            try:
                dataset = inventory.datasets[replica.dataset_name]
            except KeyError:
                result[key] = (replica.size, 0, history_record.timestamp)
                continue

            dataset_replica = site.find_dataset_replica(dataset)
            if dataset_replica is None:
                result[key] = (replica.size, 0, history_record.timestamp)
                continue

            # We don't know the history at block level - if the recorded operation was not for the full dataset, full size can be != dataset size

            result[key] = (replica.size, min(dataset_replica.size(), replica.size), dataset_replica.last_block_created())

        return result
