class FileOperation(object):
    def __init__(self, config):
        # Maximum number of tasks in a single batch
        self.batch_size = config.get('batch_size', 1)

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value

    def form_batches(self, tasks):
        """
        Organize the transfer and deletion tasks into batches in whatever way preferrable to the backend.
        Tasks can be dropped; total number of tasks in the output can be smaller than the input.
        RLFSM can decide to further break down the batch, if some files are failing.
        @params tasks  list of RLFSM.Transfer(Deletion)Task objects

        @return  List of lists of tasks
        """
        raise NotImplementedError('form_batches')

class FileQuery(object):
    _statuses = ['new', 'queued', 'active', 'done', 'failed', 'cancelled']
    STAT_NEW, STAT_QUEUED, STAT_ACTIVE, STAT_DONE, STAT_FAILED, STAT_CANCELLED = range(6)

    @staticmethod
    def status_name(val):
        try:
            return FileQuery._statuses[val - 1]
        except:
            return val

    @staticmethod
    def status_val(arg):
        try:
            return eval('FileQuery.STAT_' + arg.upper())
        except:
            return arg


    def __init__(self, config):
        pass
