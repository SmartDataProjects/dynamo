from dynamo.utils.classutil import get_instance

class FileTransferOperation(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferOperation, module, config)

    def __init__(self, config):
        # Number of files to process in single batch
        self.batch_size = config.get('batch_size', 1)

        self.dry_run = config.get('dry_run', False)

    def form_batches(self, tasks):
        """
        Organize the transfer tasks into batches in whatever way preferrable to the system.
        Tasks can be dropped; total number of tasks in the output can be smaller than the input.
        RLFSM can decide to further break down the batch, if some files are failing.
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
    _statuses = ['new', 'inprogress', 'done', 'failed']
    STAT_NEW, STAT_INPROGRESS, STAT_DONE, STAT_FAILED = range(4)

    @staticmethod
    def get_instance(module, config):
        return get_instance(FileTransferQuery, module, config)

    @staticmethod
    def status_name(val):
        try:
            return FileTransferQuery._statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('FileTransferQuery.STAT_' + arg.upper())
        except:
            return arg


    def __init__(self, config):
        pass

    def get_status(self, batch_id):
        """
        Query the transfer system about tasks in the given batch id.
        @param batch_id   Integer

        @return  [(transfer_id, status, exit code, finish time (UNIX))]
        """
        raise NotImplementedError('get_status')
