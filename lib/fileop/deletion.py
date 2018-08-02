from dynamo.fileop.base import FileOperation, FileQuery
from dynamo.utils.classutil import get_instance

class FileDeletionOperation(FileOperation):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionOperation, module, config)

    def __init__(self, config):
        FileOperation.__init__(self, config)

        # Throttling threshold
        self.max_pending_deletions = config.get('max_pending_deletions', 0xffffffff)

    def num_pending_deletions(self):
        """
        Return the number of pending deletions. Can report max_pending_deletions even when there are more.
        """
        raise NotImplementedError('num_pending_deletions')

    def start_deletions(self, batch_id, batch_tasks):
        """
        Do the deletion operation on the batch of tasks.
        @params batch_id     Integer
        @params batch_tasks  List of DeletionTask objects

        @return  {task: boolean} True if successfully submitted
        """
        raise NotImplementedError('start_deletions')

    def cancel_deletions(self, task_ids):
        """
        Cancel tasks.
        @params task_ids    List of DeletionTask ids
        """
        raise NotImplementedError('cancel_deletions')

    def cleanup(self):
        """
        Clear the inner state in case of crash recovery.
        """
        raise NotImplementedError('cleanup')

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

class FileDeletionQuery(FileQuery):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionQuery, module, config)

    def __init__(self, config):
        FileQuery.__init__(self, config)

    def get_deletion_status(self, batch_id):
        """
        Query the external agent about tasks in the given batch id.
        @param batch_id   Integer id of the deletion task batch.

        @return  [(task_id, status, exit code, message, start time (UNIX), finish time (UNIX))]
        """
        raise NotImplementedError('get_deletion_status')

    def write_deletion_history(self, history_db, task_id, history_id):
        """
        Enter whatever specific information this plugin has to the history DB.
        @param history_db  HistoryDatabase instance
        @param task_id     Deletion task id
        @param history_id  ID in the history file_deletions table
        """
        raise NotImplementedError('write_deletion_history')

    def forget_deletion_status(self, task_id):
        """
        Delete the internal record (if there is any) of the specific task.
        @param task_id   Integer id of the deletion task.
        """
        raise NotImplementedError('fotget_deletion_status')

    def forget_deletion_batch(self, batch_id):
        """
        Delete the internal record (if there is any) of the specific batch.
        @param batch_id   Integer id of the deletion task batch.
        """
        raise NotImplementedError('fotget_deletion_batch')
