"""
Define translations from text-based detox configuration to actual python expressions here
"""

from common.dataformat import Dataset, Site, DatasetReplica, BlockReplica

BOOL_TYPE, NUMERIC_TYPE, TEXT_TYPE, TIME_TYPE = range(4)

class DatasetAttr(object):
    """Extract an attribute from the dataset regardless of the type of replica passed __call__"""

    def __init__(self, attr):
        self.attr = attr
        self._is_func = callable(attr)

    def __call__(self, replica):
        if type(replica) is DatasetReplica:
            dataset = replica.dataset
        else:
            dataset = replica.block.dataset

        if self._is_func:
            return self.attr(dataset)
        else:
            return getattr(dataset, self.attr)

class ReplicaAttr(object):
    """Extract an attribute from the replica. If dataset replica is passed, switch behavior depending on _algo."""

    NOBLOCK, SUM, MAX, MIN = range(5)

    def __init__(self, attr, algo):
        self.attr = attr
        self._is_func = callable(attr)
        self._algo = algo

    def __call__(self, replica):
        if type(replica) is BlockReplica:
            if self._is_func:
                return self.attr(replica)
            else:
                return getattr(replica, self.attr)
        else:
            if self._algo == ReplicaAttr.NOBLOCK:
                if self._is_func:
                    return self.attr(replica)
                else:
                    return getattr(replica, self.attr)
            else:
                if len(replica.block_replicas) == 0:
                    # not sure if this is what we want..
                    raise RuntimeError('Empty dataset replica in SUM, MAX, or MIN')

                values = []
                for block_replica in replica.block_replicas:
                    if self._is_func:
                        values.append(self.attr(replica))
                    else:
                        values.append(getattr(replica, self.attr))

                if self._algo == ReplicaAttr.SUM:
                    return sum(values)
                elif self._algo == ReplicaAttr.MAX:
                    return max(values)
                elif self._algo == ReplicaAttr.MIN:
                    return min(values)


def dataset_has_incomplete_replica(dataset):
    for rep in replica.dataset.replicas:
        if replica_incomplete(rep):
            return True

    return False

def dataset_release(dataset):
    version = dataset.software_version
    if version[3] == '':
        return '%d_%d_%d' % version[:3]
    else:
        return '%d_%d_%d_%s' % version

def dataset_num_full_disk_copy(dataset):
    num = 0
    for rep in dataset.replicas:
        if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.is_full():
            num += 1

    return num

def dataset_num_full_copy(dataset):
    num = dataset_num_full_disk_copy(dataset)
    if dataset.on_tape == Dataset.TAPE_FULL:
        num += 1

    return num

def dataset_demand_rank(dataset):
    try:
        return dataset.demand['global_demand_rank']
    except KeyError:
        return 0.

def dataset_usage_rank(dataset):
    try:
        return dataset.demand['global_usage_rank']
    except KeyError:
        return 0.


def replica_incomplete(replica):
    if replica.is_complete:
        return False

    for block_replica in replica.block_replicas:
        if not block_replica.is_complete:
            return True

    return False

def replica_has_locked_block(replica):
    try:
        locked_blocks = replica.dataset.demand['locked_blocks']
    except KeyError:
        return False

    return replica.site in locked_blocks and len(locked_blocks[replica.site]) != 0


def replica_last_used(replica):
    try:
        last_used = replica.dataset.demand['local_usage'][replica.site].last_access
    except KeyError:
        last_used = 0

    return max(replica.last_block_created, last_used)

def replica_num_access(replica):
    try:
        return replica.dataset.demand['local_usage'][replica.site].num_access
    except KeyError:
        return 0

def replica_num_full_disk_copy_common_owner(replica):
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


replica_vardefs = {
    'dataset.name': (DatasetAttr('name'), TEXT_TYPE),
    'dataset.status': (DatasetAttr('status'), NUMERIC_TYPE, lambda v: eval('Dataset.STAT_' + v)),
    'dataset.on_tape': (DatasetAttr('on_tape'), NUMERIC_TYPE, lambda v: eval('Dataset.TAPE_' + v)),
    'dataset.size': (DatasetAttr('size'), NUMERIC_TYPE),
    'dataset.last_update': (DatasetAttr('last_update'), TIME_TYPE),
    'dataset.num_full_disk_copy': (DatasetAttr(dataset_num_full_disk_copy), NUMERIC_TYPE),
    'dataset.usage_rank': (DatasetAttr(dataset_usage_rank), NUMERIC_TYPE),
    'dataset.demand_rank': (DatasetAttr(dataset_demand_rank), NUMERIC_TYPE),
    'dataset.release': (DatasetAttr(dataset_release), TEXT_TYPE),
    'replica.is_last_transfer_source': (lambda r: r.is_full() and dataset_num_full_copy(r.dataset) == 1 and dataset_has_incomplete_replica(r.dataset), BOOL_TYPE),
    'replica.size': (lambda r: r.size(), NUMERIC_TYPE),
    'replica.incomplete': (replica_incomplete, BOOL_TYPE),
    'replica.last_block_created': (lambda r: r.last_block_created, TIME_TYPE),
    'replica.last_used': (replica_last_used, TIME_TYPE),
    'replica.num_access': (replica_num_access, NUMERIC_TYPE),
    'replica.has_locked_block': (replica_has_locked_block, BOOL_TYPE),
    'replica.owners': (lambda r: list(set(br.group.name for br in r.block_replicas if br.group is not None)), TEXT_TYPE),
    'replica.num_full_disk_copy_common_owner': (replica_num_full_disk_copy_common_owner, NUMERIC_TYPE)
}

# Variables that may change their values during a single program execution
replica_dynamic_variables = ['dataset.num_full_disk_copy', 'replica.owners', 'replica.num_full_disk_copy_common_owner']

# site variable definition: partition -> (site -> value)
site_vardefs = {
    'site.name': (lambda p: lambda s: s.name, TEXT_TYPE),
    'site.status': (lambda p: lambda s: s.status, NUMERIC_TYPE, lambda v: eval('Site.STAT_' + v)),
    'site.occupancy': (lambda p: lambda s: s.storage_occupancy([p]), NUMERIC_TYPE),
    'site.quota': (lambda p: lambda s: s.partition_quota(p), NUMERIC_TYPE),
    'never': (lambda p: lambda s: False, BOOL_TYPE),
    'always': (lambda p: lambda s: True, BOOL_TYPE)
}

required_plugins = {
    'replica_access': ['dataset.last_used', 'dataset.usage_rank', 'replica.num_access'],
    'replica_demands': ['dataset.demand_rank'],
    'dataset_request': [],
    'replica_locks': ['replica.has_locked_block']
}
