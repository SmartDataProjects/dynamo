"""
Define translations from text-based policies to actual python expressions here
"""

import re
import fnmatch

from dynamo.dataformat import Dataset, Site
from dynamo.policy.attrs import Attr, DatasetAttr, DatasetReplicaAttr, BlockReplicaAttr, ReplicaSiteAttr, SiteAttr, InvalidExpression

class DatasetHasIncompleteReplica(DatasetAttr):
    def __init__(self):
        DatasetAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, dataset):
        for rep in dataset.replicas:
            if not rep.is_complete():
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
        DatasetAttr.__init__(self, Attr.NUMERIC_TYPE)

    def rhs_map(self, expr):
        # historic mapping
        if expr == 'NONE':
            return 0
        elif expr == 'PARTIAL':
            return 2
        else:
            return 1

    def _get(self, dataset):
        on_tape = 0
        for rep in dataset.replicas:
            if rep.site.storage_type == Site.TYPE_MSS:
                if rep.is_full():
                    return 1

                on_tape = 2

        return on_tape

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
            if rep.site.status == Site.STAT_READY and rep.is_full():
                num += 1

        return num

class ReplicaSize(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, replica):
        return replica.size()

class ReplicaIncomplete(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, replica):
        if not replica.is_complete():
            return True
    
        return False

class ReplicaNumFullDiskCopyCommonOwner(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, replica):
        owners = set(br.group for br in replica.block_replicas)
        dataset = replica.dataset
        num = 0
        for rep in dataset.replicas:
            if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.is_full():
                rep_owners = set(br.group for br in rep.block_replicas)
                if len(owners & rep_owners) != 0:
                    num += 1
    
        return num

class ReplicaEnforcerProtected(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.BOOL_TYPE)

        self.required_attrs = ['enforcer_protected_replicas']

    def _get(self, replica):
        try:
            protected_replicas = replica.dataset.attr['enforcer_protected_replicas']
        except KeyError:
            return False

        return replica in protected_replicas

class ReplicaIsLastSource(DatasetReplicaAttr):
    """True if this replica is the last full disk copy and there is an ongoing transfer."""

    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.BOOL_TYPE)

    def _get(self, replica):
        if not replica.is_full():
            return False

        nfull = 0
        nincomplete = 0
        for rep in replica.dataset.replicas:
            if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.is_full():
                nfull += 1
            elif not rep.is_complete():
                nincomplete += 1

        return nfull == 1 and nincomplete != 0

class ReplicaFirstBlockCreated(DatasetReplicaAttr):
    def __init__(self):
        DatasetReplicaAttr.__init__(self, Attr.TIME_TYPE)

    def _get(self, replica):
        value = 0xffffffff
        for block_replica in replica.block_replicas:
            if block_replica.last_update < value:
                value = block_replica.last_update

        return value

class ReplicaOwner(BlockReplicaAttr):
    def __init__(self):
        BlockReplicaAttr.__init__(self, Attr.TEXT_TYPE)

    def _get(self, replica):
        if replica.group.name is None:
            return 'None'
        else:
            return replica.group.name

class ReplicaIsLocked(BlockReplicaAttr):
    def __init__(self):
        BlockReplicaAttr.__init__(self, Attr.BOOL_TYPE)

        self.required_attrs = ['locked_blocks']

    def _get(self, replica):
        try:
            locked_blocks = replica.block.dataset.attr['locked_blocks'][replica.site]
        except KeyError:
            return False

        return replica.block in locked_blocks

class ReplicaSiteStatus(ReplicaSiteAttr):
    def __init__(self):
        ReplicaSiteAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'status')

    def rhs_map(self, expr):
        return getattr(Site, 'STAT_' + expr)

class ReplicaSiteStorageType(ReplicaSiteAttr):
    def __init__(self):
        ReplicaSiteAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'storage_type')

    def rhs_map(self, expr):
        return getattr(Site, 'TYPE_' + expr)

class SiteName(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.TEXT_TYPE, attr = 'name')
        self.get_from_site = True

class SiteStatus(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'status')
        self.get_from_site = True

    def rhs_map(self, expr):
        return getattr(Site, 'STAT_' + expr)

class SiteStorageType(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE, attr = 'storage_type')
        self.get_from_site = True

    def rhs_map(self, expr):
        return getattr(Site, 'TYPE_' + expr)

class SiteOccupancy(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, sitepartition):
        return sitepartition.occupancy_fraction()

class SiteQuota(SiteAttr):
    def __init__(self):
        SiteAttr.__init__(self, Attr.NUMERIC_TYPE)

    def _get(self, site):
        return sitepartition.quota

class SiteBool(SiteAttr):
    def __init__(self, value):
        SiteAttr.__init__(self, Attr.BOOL_TYPE)
        self.value = value

    def _get(self, sitepartition):
        return self.value


replica_variables = {
    'dataset.name': DatasetName(),
    'dataset.status': DatasetStatus(),
    'dataset.on_tape': DatasetOnTape(),
    'dataset.size': DatasetAttr(Attr.NUMERIC_TYPE, 'size'),
    'dataset.last_update': DatasetAttr(Attr.TIME_TYPE, 'last_update'),
    'dataset.num_full_disk_copy': DatasetNumFullDiskCopy(),
    'dataset.usage_rank': DatasetAttr(Attr.NUMERIC_TYPE, dict_attr = 'global_usage_rank'),
    'dataset.demand_rank': DatasetAttr(Attr.NUMERIC_TYPE, dict_attr = 'global_demand_rank'),
    'dataset.release': DatasetRelease(),
    'dataset.is_latest_production_release': DatasetAttr(Attr.BOOL_TYPE, dict_attr = 'latest_production_release', dict_default = False),
    'dataset.on_protected_site': DatasetAttr(Attr.BOOL_TYPE, dict_attr = 'on_protected_site', dict_default = False),
    'replica.is_last_transfer_source': ReplicaIsLastSource(),
    'replica.size': ReplicaSize(),
    'replica.incomplete': ReplicaIncomplete(),
    'replica.last_block_created': DatasetReplicaAttr(Attr.TIME_TYPE, 'last_block_created', tuple()),
    'replica.first_block_created': ReplicaFirstBlockCreated(),
    'replica.num_access': DatasetAttr(Attr.NUMERIC_TYPE, dict_attr = 'num_access'),
    'replica.num_full_disk_copy_common_owner': ReplicaNumFullDiskCopyCommonOwner(),
    'replica.enforcer_protected': ReplicaEnforcerProtected(),
    'blockreplica.last_update': BlockReplicaAttr(Attr.TIME_TYPE, 'last_update'),
    'blockreplica.owner': ReplicaOwner(),
    'blockreplica.is_locked': ReplicaIsLocked(),
    'site.name': ReplicaSiteAttr(Attr.TEXT_TYPE, 'name'),
    'site.status': ReplicaSiteStatus(),
    'site.storage_type': ReplicaSiteStorageType()
}

site_variables = {
    'site.name': SiteName(),
    'site.status': SiteStatus(),
    'site.storage_type': SiteStorageType(),
    'site.occupancy': SiteOccupancy(),
    'site.quota': SiteQuota(),
    'never': SiteBool(False),
    'always': SiteBool(True)
}
