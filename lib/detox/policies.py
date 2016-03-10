import time

from detox.policy import DeletionPolicy
from common.dataformat import DatasetReplica, BlockReplica

deleteInvalid = DeletionPolicy('deleteInvalid', lambda rep, dem: not rep.dataset.is_valid, DeletionPolicy.DEC_DELETE, apply_type = DatasetReplica)

keepLocked = DeletionPolicy('keepLocked', lambda rep, dem: rep.block in dem.locked_blocks, DeletionPolicy.DEC_KEEP_OVERRIDE, apply_type = BlockReplica)

deleteOld = DeletionPolicy('deleteOld', lambda rep, dem: rep.dataset.last_accessed > 0 and rep.dataset.last_accessed < time.time() - 1.5 * 365 * 24 * 3600, DeletionPolicy.DEC_DELETE, apply_type = DatasetReplica)

deleteExcess = DeletionPolicy('deleteExcess', lambda rep, dem: len(rep.dataset.replicas) > 1, DeletionPolicy.DEC_DELETE)
