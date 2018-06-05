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
