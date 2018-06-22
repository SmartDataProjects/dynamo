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

    def schedule_deletions(self, replica_list, operation_id, comments = ''): #override
        LOG.info('Ignoring deletion schedule of %d replicas (operation %d)', len(replica_list), operation_id)

        result = []

        for replica, block_replicas in replica_list:
            clone_replica = DatasetReplica(replica.dataset, replica.site)
            clone_replica.copy(replica)

            if block_replicas is None:
                result.append((clone_replica, None))
            else:
                clone_block_replicas = []
    
                for block_replica in block_replicas:
                    clone_block_replica = BlockReplica(block_replica.block, block_replica.site, block_replica.group)
                    clone_block_replica.copy(block_replica)
                    clone_block_replica.last_update = int(time.time())
                    clone_block_replicas.append(clone_block_replica)
                    
                result.append((clone_replica, clone_block_replicas))

        return result
