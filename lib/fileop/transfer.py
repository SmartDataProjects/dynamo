from dynamo.fileop.base import FileOperation, FileQuery
from dynamo.utils.classutil import get_instance

class FileTransferOperation(FileOperation):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferOperation, module, config)
    
    def __init__(self, config):
        FileOperation.__init__(self, config)

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

        @return  [(task_id, status, exit code, start time (UNIX), finish time (UNIX))]
        """
        raise NotImplementedError('get_transfer_status')

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
