import sys
from common.dataformat.exceptions import IntegrityError

class SitePartition(object):
    """State of a partition at a site."""

    __slots__ = ['site', 'partition', 'quota', 'replicas']

    def __init__(self, site, partition):
        self.site = site
        self.partition = partition
        self.quota = 0.
        # {dataset_replica: set(block_replicas) or None (if all blocks are in)}
        self.replicas = {}

    def set_quota(self, quota):
        if self.partition.parent is not None:
            # this is a subpartition. Update the parent partition quota
            if quota < 0:
                # quota < 0 -> infinite. This partition cannot be a subpartition
                raise IntegrityError('Infinite quota set for a subpartition')

            self.site.partitions[self.partition.parent].quota += quota - self.quota

        self.quota = quota

    def occupancy_fraction(self, physical = True):
        if self.quota == 0.:
            return sys.float_info.max
        elif self.quota < 0.:
            return 0.
        else:
            total_size = 0.
            for replica, block_replicas in self.replicas.iteritems():
                if block_replicas is None:
                    total_size += replica.size(physica = physical)
                elif physical:
                    total_size += sum(br.size for br in block_replicas)
                else:
                    total_size += sum(br.block.size for br in block_replicas)

            return total_size / self.quota


class Site(object):
    """Represents a site. Owns lists of dataset and block replicas, which are organized into partitions."""

    __slots__ = ['name', 'host', 'storage_type', 'backend',
        'storage', 'cpu', 'status',
        '_dataset_replicas', 'partitions']

    TYPE_DISK, TYPE_MSS, TYPE_BUFFER, TYPE_UNKNOWN = range(1, 5)
    STAT_READY, STAT_WAITROOM, STAT_MORGUE, STAT_UNKNOWN = range(1, 5)

    @staticmethod
    def storage_type_val(arg):
        if type(arg) is str:
            arg = arg.lower()
            if arg == 'disk':
                return Site.TYPE_DISK
            elif arg == 'mss':
                return Site.TYPE_MSS
            elif arg == 'buffer':
                return Site.TYPE_BUFFER
            elif arg == 'unknown':
                return Site.TYPE_UNKNOWN

        else:
            return arg

    @staticmethod
    def storage_type_name(arg):
        if type(arg) is int:
            if arg == Site.TYPE_DISK:
                return 'disk'
            elif arg == Site.TYPE_MSS:
                return 'mss'
            elif arg == Site.TYPE_BUFFER:
                return 'buffer'
            elif arg == Site.TYPE_UNKNOWN:
                return 'unknown'

        else:
            return arg

    @staticmethod
    def status_val(arg):
        if type(arg) is str:
            arg = arg.lower()
            if arg == 'ready':
                return Site.STAT_READY
            elif arg == 'waitroom':
                return Site.STAT_WAITROOM
            elif arg == 'morgue':
                return Site.STAT_MORGUE
            elif arg == 'unknown':
                return Site.STAT_UNKNOWN

        else:
            return arg

    @staticmethod
    def status_name(arg):
        if type(arg) is int:
            if arg == Site.STAT_READY:
                return 'ready'
            elif arg == Site.STAT_WAITROOM:
                return 'waitroom'
            elif arg == Site.STAT_MORGUE:
                return 'morgue'
            elif arg == Site.STAT_UNKNOWN:
                return 'unknown'

        else:
            return arg

    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', storage = 0., cpu = 0., status = STAT_UNKNOWN):
        self.name = name
        self.host = host
        if type(storage_type) is str:
            storage_type = Site.storage_type_val(storage_type)
        self.storage_type = storage_type
        self.backend = backend
        self.storage = storage # in TB
        self.cpu = cpu # in kHS06
        self.status = status

        self._dataset_replicas = {} # {Dataset: [DatasetReplica]}

        self.partitions = {} # {Partition: SitePartition}

    def __str__(self):
        return 'Site %s (host=%s, storage_type=%s, backend=%s, storage=%d, cpu=%f, status=%s)' % \
            (self.name, self.host, Site.storage_type_name(self.storage_type), self.backend, self.storage, self.cpu, Site.status_name(self.status))

    def __repr__(self):
        return 'Site(\'%s\', host=\'%s\', storage_type=%d, backend=\'%s\', storage=%d, cpu=%f, status=%d)' % \
            (self.name, self.host, self.storage_type, self.backend, self.storage, self.cpu, self.status)

    def find_dataset_replica(self, dataset):
        try:
            return self._dataset_replicas[dataset]
        except KeyError:
            return None

    def find_block_replica(self, block):
        if type(block).__name__ == 'Block':
            try:
                dataset_replica = self._dataset_replicas[block.dataset]
                return dataset_replica.find_block_replica(block)
            except KeyError:
                return None
        else:
            # very inefficient operation
            for dataset_replica in self._dataset_replicas.itervalues():
                for block_replica in dataset_replica.block_replicas:
                    if block_replica.block.name == block:
                        return block_replica

            return None

    def replica_iter(self):
        return self._dataset_replicas.itervalues()

    def add_dataset_replica(self, replica):
        self._dataset_replicas[replica.dataset] = replica
        self._dataset_replicas[replica.dataset.name] = replica

        for partition, site_partition in self.partitions.iteritems():
            block_replicas = set()
            for block_replica in replica.block_replicas:
                if partition.contains(block_replica):
                    block_replicas.add(block_replica)

            if len(block_replicas) == 0:
                continue

            if block_replicas == replica.block_replicas:
                site_partition.replicas[replica] = None
            else:
                site_partition.replicas[replica] = block_replicas

    def update_partitioning(self, replica):
        for partition, site_partition in self.partitions.iteritems():
            try:
                block_replicas = site_partition.replicas[replica]
            except KeyError:
                block_replicas = set()

            if block_replicas is None:
                # previously, was all contained - need to check again
                self.add_dataset_replica(replica)
            else:
                # remove block replicas that were deleted
                deleted_replicas = block_replicas - replica.block_replicas
                block_replicas -= deleted_replicas

                # add new block replicas
                new_replicas = replica.block_replicas - block_replicas
                for block_replica in new_replicas:
                    if partition.contains(block_replica):
                        block_replicas.add(block_replica)
               
                if len(block_replicas) == 0:
                    try:
                        site_partition.replicas.pop(replica)
                    except KeyError:
                        pass
                else:
                    site_partition.replicas[replica] = block_replicas

    def remove_dataset_replica(self, replica):
        self._dataset_replicas.pop(replica.dataset)
        self._dataset_replicas.pop(replica.dataset.name)

        for site_partition in self.partitions.itervalues():
            try:
                site_partition.replicas.pop(replica)
            except KeyError:
                pass
