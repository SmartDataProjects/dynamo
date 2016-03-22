import time

from detox.policy import DeletionPolicy
from common.dataformat import DatasetReplica, BlockReplica
import common.configuration as config

# KEEP_OVERRIDE if replica is not complete
keepIncomplete = DeletionPolicy('keepIncomplete', lambda rep, dem: not rep.is_complete, DeletionPolicy.DEC_KEEP_OVERRIDE)

# KEEP_OVERRIDE if block is locked
keepLocked = DeletionPolicy('keepLocked', lambda rep, dem: len(set([b.name in rep.dataset.blocks]) & set(dem.get_demand(rep.dataset).locked_blocks)) != 0, DeletionPolicy.DEC_KEEP_OVERRIDE)

# KEEP_OVERRIDE if replica is custodial
keepCustodial = DeletionPolicy('keepCustodial', lambda rep, dem: rep.is_custodial, DeletionPolicy.DEC_KEEP_OVERRIDE)

# KEEP_OVERRIDE if replica is the last copy and dataset is not on tape
keepDiskOnly = DeletionPolicy('keepDiskOnly', lambda rep, dem: rep.is_last_copy() and not rep.dataset.on_tape, DeletionPolicy.DEC_KEEP_OVERRIDE)

# KEEP_OVERRIDE if site occupancy is less than target
keepTargetOccupancy = DeletionPolicy('keepTargetOccupancy', lambda rep, dem: rep.site.occupancy() < 0.85, DeletionPolicy.DEC_KEEP_OVERRIDE)

# DELETE if dataset replica is partial
deletePartial = DeletionPolicy('deletePartial', lambda rep, dem: rep.is_partial, DeletionPolicy.DEC_DELETE)

# DELETE if dataset last_accessed is more than 1.5 years ago
deleteOld = DeletionPolicy('deleteOld', lambda rep, dem: rep.dataset.last_accessed > 0 and rep.dataset.last_accessed < time.time() - 1.5 * 365 * 24 * 3600, DeletionPolicy.DEC_DELETE)

# DELETE if this is the least popular dataset at the site
deleteUnpopular = DeletionPolicy('deleteUnpopular', lambda rep, dem: dem.popularity_score == max([dem.get_demand(d).popularity_score for d in rep.site.datasets]), DeletionPolicy.DEC_DELETE)
