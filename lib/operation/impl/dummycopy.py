import time
import logging

from dynamo.operation.copy import CopyInterface
from dynamo.dataformat import DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class DummyCopyInterface(CopyInterface):
    """
    CopyInterface that actually does nothing.
    """

    def __init__(self, config = None):
        CopyInterface.__init__(self, config)

    def schedule_copies(self, replica_list, operation_id, comments = ''): #override
        LOG.info('Ignoring copy schedule of %d replicas (operation %d)', len(replica_list), operation_id)

        result = []

        for replica in replica_list:
            clone_replica = DatasetReplica(replica.dataset, replica.site)
            clone_replica.copy(replica)
            result.append(clone_replica)
            
            for block_replica in replica.block_replicas:
                clone_block_replica = BlockReplica(block_replica.block, block_replica.site, block_replica.group)
                clone_block_replica.copy(block_replica)
                clone_block_replica.last_update = int(time.time())
                clone_replica.block_replicas.add(clone_block_replica)

        return result
