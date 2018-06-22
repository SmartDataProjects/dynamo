import time
import logging

from dynamo.operation.copy import CopyInterface
from dynamo.dataformat import DatasetReplica, BlockReplica, OperationalError
from dynamo.fileop.rlfsm import RLFSM

LOG = logging.getLogger(__name__)

class FODCopyInterface(CopyInterface):
    """
    CopyInterface using the Dynamo FOD.
    """

    def __init__(self, config = None):
        CopyInterface.__init__(self, config)
        self.rlfsm = RLFSM(config.get('fod', None))

    def set_read_only(self, value = True): #override
        self._read_only = value
        self.rlfsm.set_read_only(value)

    def schedule_copies(self, replica_list, operation_id, comments = ''): #override
        sites = set(r.site for r in replica_list)
        if len(sites) != 1:
            raise OperationalError('schedule_copies should be called with a list of replicas at a single site.')

        LOG.info('Scheduling copy of %d replicas to %s using RLFSM (operation %d)', len(replica_list), list(sites)[0], operation_id)

        result = []

        for replica in replica_list:
            # Function spec is to return clones (so that if specific block fails to copy, we can return a dataset replica without the block)
            clone_replica = DatasetReplica(replica.dataset, replica.site)
            clone_replica.copy(replica)
            result.append(clone_replica)

            for block_replica in replica.block_replicas:
                LOG.debug('Subscribing files for %s', str(block_replica))

                if block_replica.file_ids is None:
                    LOG.debug('No file to subscribe for %s', str(block_replica))
                    return
        
                all_files = block_replica.block.files
                missing_files = all_files - block_replica.files()

                self.rlfsm.subscribe_files(block_replica.site, missing_files)

                clone_block_replica = BlockReplica(block_replica.block, block_replica.site, block_replica.group)
                clone_block_replica.copy(block_replica)
                clone_block_replica.last_update = int(time.time())
                clone_replica.block_replicas.add(clone_block_replica)

        # no external dependency - everything is a success
        return result

    def copy_status(self, operation_id): #override
        raise NotImplementedError('copy_status')
