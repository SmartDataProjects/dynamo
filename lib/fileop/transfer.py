from dynamo.fileop.base import FileOperation, FileQuery
from dynamo.utils.classutil import get_instance
from dynamo.dataformat import File, ConfigurationError

class FileTransferOperation(FileOperation):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferOperation, module, config)
    
    def __init__(self, config):
        FileOperation.__init__(self, config)

        # Throttling threshold
        self.max_pending_transfers = config.get('max_pending_transfers', 0xffffffff)

        # Checksum algorithm to use (optional)
        self.checksum_algorithm = config.get('checksum_algorithm', '')
        if self.checksum_algorithm:
            try:
                self.checksum_index = File.checksum_algorithms.index(self.checksum_algorithm)
            except ValueError:
                raise ConfigurationError('Checksum algorithm %s not supported by File object.' % self.checksum_algorithm)

    def num_pending_transfers(self):
        """
        Return the number of pending transfers. Can report max_pending_transfers even when there are more.
        """
        raise NotImplementedError('num_pending_transfers')

    def start_transfers(self, batch_id, batch_tasks):
        """
        Do the transfer operation on the batch of tasks.
        @params batch_id     Integer
        @params batch_tasks  List of TransferTask objects

        @return  {task: boolean} True for submission success
        """
        raise NotImplementedError('start_transfers')

    def cancel_transfers(self, task_ids):
        """
        Cancel tasks.
        @params task_ids    List of TransferTask ids
        """
        raise NotImplementedError('cancel_transfers')

    def cleanup(self):
        """
        Clear the inner state in case of crash recovery.
        """
        raise NotImplementedError('cleanup')

class FileTransferQuery(FileQuery):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferQuery, module, config)

    def __init__(self, config):
        FileQuery.__init__(self, config)

    def get_transfer_status(self, batch_id):
        """
        Query the external agent about tasks in the given batch id.
        @param batch_id   Integer id of the transfer task batch.

        @return  [(task_id, status, exit code, message, start time (UNIX), finish time (UNIX))]
        """
        raise NotImplementedError('get_transfer_status')

    def write_transfer_history(self, history_db, task_id, history_id):
        """
        Enter whatever specific information this plugin has to the history DB.
        @param history_db  HistoryDatabase instance
        @param task_id     Transfer task id
        @param history_id  ID in the history file_transfers table
        """
        raise NotImplementedError('write_transfer_history')

    def forget_transfer_status(self, task_id):
        """
        Delete the internal record (if there is any) of the specific task.
        @param task_id   Integer id of the transfer task.
        """
        raise NotImplementedError('fotget_transfer_status')

    def forget_transfer_batch(self, batch_id):
        """
        Delete the internal record (if there is any) of the specific batch.
        @param batch_id   Integer id of the transfer task batch.
        """
        raise NotImplementedError('fotget_transfer_batch')
