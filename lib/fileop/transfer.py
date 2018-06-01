from dynamo.utils.classutil import get_instance

class FileTransferOperation(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferOperation, module, config)

    def __init__(self, config):
        # Number of files to process in single batch (Max used 4000)
        self.batch_size = config.batch_size

    def form_batches(self, tasks):
        """
        Organize the transfer tasks into batches in whatever way preferrable to the system.
        Tasks can be dropped; total number of tasks in the output can be smaller than the input.
        @params tasks  list of RLFSM.TransferTask objects

        @return  List of lists of tasks
        """
        raise NotImplementedError('form_batches')

    def start_transfers(self, batch_id, batch_tasks):
        """
        Do the transfer operation on the batch of tasks.
        @params batch_id     Integer
        @params batch_tasks  List of TransferTask objects
        """
        raise NotImplementedError('start_transfers')

class FileTransferQuery(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferQuery, module, config)

    STAT_NEW, STAT_INPROGRESS, STAT_DONE, STAT_FAILED = range(4)

    def __init__(self, config):
        pass

    def get_status(self, batch_id):
        """
        Query the transfer system about tasks in the given batch id.
        @param batch_id   Integer

        @return  [(transfer_id, status, exit code, finish time (UNIX))]
        """
        raise NotImplementedError('get_status')
