class FileOperation(object):
    def __init__(self, config):
        # Number of files to process in single batch
        self.batch_size = config.get('batch_size', 1)

        self.dry_run = config.get('dry_run', False)

    def form_batches(self, tasks):
        """
        Organize the transfer and deletion tasks into batches in whatever way preferrable to the system.
        Tasks can be dropped; total number of tasks in the output can be smaller than the input.
        RLFSM can decide to further break down the batch, if some files are failing.
        @params tasks  list of RLFSM.Transfer(Deletion)Task objects

        @return  List of lists of tasks
        """
        raise NotImplementedError('form_batches')

class FileQuery(object):
    _statuses = ['new', 'inprogress', 'done', 'failed', 'cancelled']
    STAT_NEW, STAT_INPROGRESS, STAT_DONE, STAT_FAILED, STAT_CANCELLED = range(5)

    @staticmethod
    def status_name(val):
        try:
            return FileQuery._statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('FileQuery.STAT_' + arg.upper())
        except:
            return arg


    def __init__(self, config):
        pass
