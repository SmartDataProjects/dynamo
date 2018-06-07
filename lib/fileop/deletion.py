from dynamo.fileop.base import FileOperation, FileQuery
from dynamo.utils.classutil import get_instance

class FileDeletionOperation(FileOperation):
    @staticmethod
    def get_instance(module, config):
        return get_instance(FileDeletionOperation, module, config)

    def __init__(self, config):
        FileOperation.__init__(self, config)

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

        @return  [(task_id, status, exit code, start time (UNIX), finish time (UNIX))]
        """
        raise NotImplementedError('get_transfer_status')

    def forget_deletion_status(self, batch_id, task_id):
        """
        Delete the internal record (if there is any) of the specific task.
        @param batch_id  Integer id of the deletion task batch.
        @param task_id   Integer id of the deletion task.
        """
        raise NotImplementedError('fotget_transfer_status')
