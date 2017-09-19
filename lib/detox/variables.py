"""
Define translations from text-based detox configuration to actual python expressions here
"""

import re
import fnmatch
from common.dataformat import Dataset, Site
from detox.attrs import Attr, DatasetAttr, DatasetReplicaAttr, BlockReplicaAttr, SiteAttr, InvalidExpression

class DatasetHasIncompleteReplica(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, dataset):
        for rep in dataset.replicas:
            if not rep.is_complete:
                return True

            for block_replica in rep.block_replicas:
                if not block_replica.is_complete:
                    return True

        return False

class DatasetName(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.TEXT_TYPE, attr = 'name')

    def rhs_map(self, expr):
        if not re.match('/[^/]+/[^/]+/[^/]+', expr):
            raise InvalidExpression('Invalid dataset name ' + expr)
        
        if '*' in expr or '?' in expr:
            return re.compile(fnmatch.translate(expr))
        else:
            return expr

class DatasetStatus(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'status')

    def rhs_map(self, expr):
        return getattr(Dataset, 'STAT_' + expr)

class DatasetOnTape(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'on_tape')

    def rhs_map(self, expr):
        return getattr(Dataset, 'TAPE_' + expr)

class DatasetRelease(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.TEXT_TYPE)

    def _get(self, dataset):
        version = dataset.software_version
        if version[3] == '':
            return '%d_%d_%d' % version[:3]
        else:
            return '%d_%d_%d_%s' % version

class DatasetNumFullDiskCopy(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, dataset):
        num = 0
        for rep in dataset.replicas:
            if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.is_full():
                num += 1

        return num

class DatasetNumFullCopy(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, dataset):
        num = 0
        for rep in dataset.replicas:
            if rep.is_full():
                num += 1

        return num

class DatasetDemandRank(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, dataset):
        try:
            return dataset.demand['global_demand_rank']
        except KeyError:
            return 0.

class DatasetUsageRank(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, dataset):
        try:
            return dataset.demand['global_usage_rank']
        except KeyError:
            return 0.

class ReplicaSize(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, replica):
        return replica.size()

class ReplicaIncomplete(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, replica):
        if not replica.is_complete:
            return True
    
        for block_replica in replica.block_replicas:
            if not block_replica.is_complete:
                return True
    
        return False

class ReplicaLastUsed(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.TIME_TYPE)

    def _get(self, replica):
        try:
            last_used = replica.dataset.demand['local_usage'][replica.site].last_access
        except KeyError:
            last_used = 0
    
        return max(replica.last_block_created, last_used)

class ReplicaNumAccess(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, replica):
        try:
            return replica.dataset.demand['local_usage'][replica.site].num_access
        except KeyError:
            return 0

class ReplicaNumFullDiskCopyCommonOwner(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, replica):
        owners = set(br.group for br in replica.block_replicas if br.group is not None)
        dataset = replica.dataset
        num = 0
        for rep in dataset.replicas:
            if rep == replica:
                num += 1
                continue
    
            if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.is_full():
                rep_owners = set(br.group for br in rep.block_replicas if br.group is not None)
                if len(owners & rep_owners) != 0:
                    num += 1
    
        return num

class ReplicaIsLastSource(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, replica):
        if not replica.is_full():
            return False

        nfull = 0
        nincomplete = 0
        for rep in replica.dataset.replicas:
            if rep.is_full():
                nfull += 1

            if not rep.is_complete:
                nincomplete += 1
            else:
                for block_replica in rep.block_replicas:
                    if not block_replica.is_complete:
                        nincomplete += 1
                        break

        return nfull == 1 and nincomplete != 0

class ReplicaOwner(BlockReplicaAttr):
    def __init__(self):
        BlockReplicaAttr.__init__(self, Attr.TEXT_TYPE)

    def _get(self, replica):
        if replica.group is None:
            return 'None'
        else:
            return replica.group.name

class ReplicaIsLocked(BlockReplicaAttr):
    def __init__(self):
        BlockReplicaAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, replica):
        try:
            locked_blocks = replica.block.dataset.demand['locked_blocks'][replica.site]
        except KeyError:
            return False

        return replica.block in locked_blocks

class SiteStatus(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'status')

    def rhs_map(self, expr):
        return getattr(Site, 'STAT_' + expr)

class SiteOccupancy(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, site):
        return site.storage_occupancy([self.partition])

class SiteQuota(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, site):
        return site.partition_quota([self.partition])

class SiteBool(SiteAttr):
    def __init__(self, value):
        SiteAttr.__init__(self, Attr.BOOL_TYPE)
        self.value = value

    def _get(self, site):
        return self.value


replica_variables = {
    'dataset.name': DatasetName(),
    'dataset.status': DatasetStatus(),
    'dataset.on_tape': DatasetOnTape(),
    'dataset.size': DatasetAttr(Attr.NUMERIC_TYPE, 'size'),
    'dataset.last_update': DatasetAttr(Attr.TIME_TYPE, 'last_update'),
    'dataset.num_full_disk_copy': DatasetNumFullDiskCopy(),
    'dataset.usage_rank': DatasetUsageRank(),
    'dataset.demand_rank': DatasetDemandRank(),
    'dataset.release': DatasetRelease(),
    'replica.is_last_transfer_source': ReplicaIsLastSource(),
    'replica.size': ReplicaSize(),
    'replica.incomplete': ReplicaIncomplete(),
    'replica.last_block_created': DatasetReplicaAttr(Attr.TIME_TYPE, 'last_block_created'),
    'replica.last_used': ReplicaLastUsed(),
    'replica.num_access': ReplicaNumAccess(),
    'replica.num_full_disk_copy_common_owner': ReplicaNumFullDiskCopyCommonOwner(),
    'blockreplica.last_update': BlockReplicaAttr(Attr.TIME_TYPE, 'last_update'),
    'blockreplica.owner': ReplicaOwner(),
    'blockreplica.is_locked': ReplicaIsLocked()
}

# Variables that may change their values during a single program execution
replica_dynamic_variables = [
    'dataset.num_full_disk_copy',
    'replica.owners',
    'replica.num_full_disk_copy_common_owner',
    'blockreplica.last_update',
    'blockreplica.owner'
]

# site variable definition: partition -> (site -> value)
site_variables = {
    'site.name': SiteAttr(Attr.TEXT_TYPE, 'name'),
    'site.status': SiteStatus(),
    'site.occupancy': SiteOccupancy(),
    'site.quota': SiteQuota(),
    'never': SiteBool(False),
    'always': SiteBool(True)
}

required_plugins = {
    'replica_access': ['replica.last_used', 'dataset.usage_rank', 'replica.num_access'],
    'replica_demands': ['dataset.demand_rank'],
    'dataset_request': [],
    'replica_locks': ['blockreplica.is_locked']
}
