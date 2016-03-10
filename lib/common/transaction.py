from common.dataformat import DatasetReplica, BlockReplica
from common.interface.classes import default_interface

class TransactionManager(object):
    """
    Manages copy and deletion of data.
    """

    def __init__(self, copy_cls = None, deletion_cls = None):
        if copy_cls:
            self.copy = copy_cls()
        else:
            self.copy = default_interface['copy']()

        if deletion_cls:
            self.deletion = deletion_cls()
        else:
            self.deletion = default_interface['deletion']()

    def delete(self, replica):
        if type(replica) is DatasetReplica:
            self.deletion.schedule_deletion(replica.dataset, replica.site)

        elif type(replica) is BlockReplica:
            self.deletion.schedule_deletion(replica.block, replica.site)
