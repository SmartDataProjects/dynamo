"""
Define translations from text-based detox configuration to actual python expressions here
"""

from common.dataformat import Dataset, Site, DatasetReplica

BOOL_TYPE, NUMERIC_TYPE, TEXT_TYPE, TIME_TYPE = range(4)

def replica_incomplete(replica):
    if replica.is_complete:
        return False

    for block_replica in replica.block_replicas:
        if not block_replica.is_complete:
            return True

    return False

def replica_has_locked_block(replica):
    for block_replica in replica.block_replicas:
        if block_replica in replica.dataset.demand.locked_blocks:
            return True

    return False

def replica_dataset_release(replica):
    version = replica.dataset.software_version
    if version[3] == '':
        return '%d_%d_%d' % version[:3]
    else:
        return '%d_%d_%d_%s' % version

def dataset_num_full_disk_copy(replica):
    dataset = replica.dataset
    num = 0
    for rep in dataset.replicas:
        if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.site.active != Site.ACT_IGNORE and rep.is_full():
            num += 1

    return num

def replica_num_full_disk_copy_common_owner(replica):
    owners = set(br.group for br in replica.block_replicas if br.group is not None)
    dataset = replica.dataset
    num = 0
    for rep in dataset.replicas:
        if rep == replica:
            num += 1
            continue

        if rep.site.storage_type == Site.TYPE_DISK and rep.site.status == Site.STAT_READY and rep.site.active != Site.ACT_IGNORE and rep.is_full():
            rep_owners = set(br.group for br in rep.block_replicas if br.group is not None)
            if len(owners & rep_owners) != 0:
                num += 1

    return num

replica_vardefs = {
    'dataset.name': (lambda r: r.dataset.name, TEXT_TYPE),
    'dataset.status': (lambda r: r.dataset.status, NUMERIC_TYPE, lambda v: eval('Dataset.STAT_' + v)),
    'dataset.on_tape': (lambda r: r.dataset.on_tape, NUMERIC_TYPE, lambda v: eval('Dataset.TAPE_' + v)),
    'dataset.last_update': (lambda r: r.dataset.last_update, TIME_TYPE),
    'dataset.num_full_disk_copy': (dataset_num_full_disk_copy, NUMERIC_TYPE),
    'dataset.usage_rank': (lambda r: r.dataset.demand.global_usage_rank, NUMERIC_TYPE),
    'dataset.release': (replica_dataset_release, TEXT_TYPE),
    'replica.incomplete': (replica_incomplete, BOOL_TYPE),
    'replica.last_block_created': (lambda r: r.last_block_created, TIME_TYPE),
    'replica.last_used': (lambda r: max(r.dataset.last_update, r.last_access()), TIME_TYPE),
    'replica.num_access': (lambda r: len(r.accesses[DatasetReplica.ACC_LOCAL]) + len(r.accesses[DatasetReplica.ACC_REMOTE]), NUMERIC_TYPE),
    'replica.has_locked_block': (replica_has_locked_block, BOOL_TYPE),
    'replica.owners': (lambda r: list(set(br.group.name for br in r.block_replicas if br.group is not None)), TEXT_TYPE),
    'replica.num_full_disk_copy_common_owner': (replica_num_full_disk_copy_common_owner, NUMERIC_TYPE)
}

# Variables that may change their values during a single program execution
replica_dynamic_variables = ['dataset.num_full_disk_copy', 'replica.owners', 'replica.num_full_disk_copy_common_owner']

# Variables that use dataset replica accesses
replica_access_variables = ['dataset.last_used', 'dataset.usage_rank', 'replica.num_access']
# Variables that use dataset requests
replica_request_variables = []
# Variables that use locks
replica_lock_variables = ['replica.has_locked_block']

# site variable definition: partition -> (site -> value)
site_vardefs = {
    'site.name': (lambda p: lambda s: s.name, TEXT_TYPE),
    'site.status': (lambda p: lambda s: s.status, NUMERIC_TYPE, lambda v: eval('Site.STAT_' + v)),
    'site.active': (lambda p: lambda s: s.active, NUMERIC_TYPE, lambda v: eval('Site.ACT_' + v)),
    'site.occupancy': (lambda p: lambda s: s.storage_occupancy([p]), NUMERIC_TYPE),
    'site.quota': (lambda p: lambda s: s.partition_quota(p), NUMERIC_TYPE),
    'never': (lambda p: lambda s: False, BOOL_TYPE),
    'always': (lambda p: lambda s: True, BOOL_TYPE)
}
