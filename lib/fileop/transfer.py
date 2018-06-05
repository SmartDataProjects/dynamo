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
        """
        raise NotImplementedError('start_transfers')

class FileTransferQuery(FileQuery):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferQuery, module, config)

    def __init__(self, config):
        FileQuery.__init__(self, config)
