import time
import logging

from dynamo.operation.deletion import DeletionInterface
from dynamo.dataformat import DatasetReplica, BlockReplica
from dynamo.fileop.rlfsm import RLFSM

LOG = logging.getLogger(__name__)

class RLFSMDeletionInterface(DeletionInterface):
    """
    DeletionInterface using the Dynamo RLFSM.
    """

    def __init__(self, config = None):
        DeletionInterface.__init__(self, config)
        self.rlfsm = RLFSM(config.get('rlfsm', None))

    def set_read_only(self, value = True): #override
        self._read_only = value
        self.rlfsm.set_read_only(value)

    def schedule_deletions(self, replica_list, operation_id, comments = ''): #override
        sites = set(r.site for r, b in replica_list)
        if len(sites) != 1:
            raise OperationalError('schedule_copies should be called with a list of replicas at a single site.')

        site = list(sites)[0]

        LOG.info('Scheduling deletion of %d replicas from %s using RLFSM (operation %d)', len(replica_list), site.name, operation_id)

        clones = []

        for dataset_replica, block_replicas in replica_list:
            if block_replicas is None:
                to_delete = dataset_replica.block_replicas
            else:
                to_delete = block_replicas

            for block_replica in to_delete:
                self.rlfsm.desubscribe_files(block_replica.site, block_replica.files())

            # No external dependency -> all operations are successful

            clone_replica = DatasetReplica(dataset_replica.dataset, dataset_replica.site)
            clone_replica.copy(dataset_replica)

            if block_replicas is None:
                clones.append((clone_replica, None))
            else:
                clones.append((clone_replica, []))
                for block_replica in block_replicas:
                    clone_block_replica = BlockReplica(block_replica.block, block_replica.site)
                    clone_block_replica.copy(block_replica)
                    clone_block_replica.last_update = int(time.time())
                    clones[-1][1].append(clone_block_replica)

        return clones

    def deletion_status(self, operation_id): #override
        raise NotImplementedError('deletion_status')
