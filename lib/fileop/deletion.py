from dynamo.utils.classutil import get_instance

class FileDeletionOperation(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionOperation, module, config)

    def __init__(self, config):
        # Number of files to process in single batch (Max used 4000)
        self.batch_size = config.batch_size

        self.dry_run = config.get('dry_run', False)

    def form_batches(self, tasks):
        """
        Organize the deletion tasks into batches in whatever way preferrable to the system.
        Tasks can be dropped; total number of tasks in the output can be smaller than the input.
        @params tasks  list of RLFSM.DeletionTask objects

        @return  List of lists of tasks
        """
        raise NotImplementedError('form_batches')

    def start_deletions(self, batch_id, batch_tasks):
        """
        Do the deletion operation on the batch of tasks.
        @params batch_id     Integer
        @params batch_tasks  List of DeletionTask objects
        """
        raise NotImplementedError('start_deletions')

class DirDeletionOperation(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionOperation, module, config)

    def __init__(self, config):
        # Number of files to process in single batch (Max used 4000)
        self.batch_size = config.batch_size

    def execute(self, paths):
        """
        Execute directory deletions.
        @param paths  List of physical directory names
        """
        raise NotImplementedError('execute')

class FileDeletionQuery(object):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionQuery, module, config)

    STAT_NEW, STAT_INPROGRESS, STAT_DONE, STAT_FAILED = range(4)

    def __init__(self, config):
        pass

    def get_status(self, batch_id):
        """
        Query the deletion system about tasks in the given batch id.
        @param batch_id   Integer

        @return  [(deletion_id, status, exit code, finish time (UNIX))]
        """
        raise NotImplementedError('get_status')
