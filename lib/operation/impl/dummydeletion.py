import time
import logging

from dynamo.operation.deletion import DeletionInterface
from dynamo.dataformat import DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class DummyDeletionInterface(DeletionInterface):
    """
    DeletionInterface that actually does nothing.
    """

    def __init__(self, config = None):
        DeletionInterface.__init__(self, config)

    def schedule_deletion(self, replica, comments = ''): #override
        LOG.info('Ignoring deletion schedule of %s', str(replica))

        if type(replica) is DatasetReplica:
            return {0: (True, replica.site, [replica.dataset])}
        else:
            return {0: (True, replica.site, [replica.block])}

    def schedule_copies(self, replica_list, comments = ''): #override
        LOG.info('Ignoring deletion schedule of %d replicas', len(replica_list))

        clones = []

        for dataset_replica, block_replicas in replica_list:
            if block_replicas is None:
                clones.append((clone_replica, None))
            else:
                clones.append((clone_replica, []))
                for block_replica in block_replicas:
                    clone_block_replica = BlockReplica(block_replica.block, block_replica.site)
                    clone_block_replica.copy(block_replica)
                    clone_block_replica.last_update = int(time.time())
                    clones[-1][1].append(clone_block_replica)

        return result
