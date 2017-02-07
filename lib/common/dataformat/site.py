class Site(object):
    TYPE_DISK, TYPE_MSS, TYPE_BUFFER, TYPE_UNKNOWN = range(1, 5)
    STAT_READY, STAT_WAITROOM, STAT_MORGUE, STAT_UNKNOWN = range(1, 5)
    ACT_IGNORE, ACT_AVAILABLE, ACT_NOCOPY = range(3)

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

    @staticmethod
    def activestate_val(arg):
        if type(arg) is str:
            arg = arg.lower()
            if arg == 'ignore':
                return Site.ACT_IGNORE
            elif arg == 'available':
                return Site.ACT_AVAILABLE
            elif arg == 'nocopy':
                return Site.ACT_NOCOPY

        else:
            return arg

    @staticmethod
    def activestate_name(arg):
        if type(arg) is int:
            if arg == Site.ACT_IGNORE:
                return 'ignore'
            elif arg == Site.ACT_AVAILABLE:
                return 'available'
            elif arg == Site.ACT_NOCOPY:
                return 'nocopy'

        else:
            return arg

    class Partition(object):
        """
        Defines storage partitioning.
        _partitioning: A function that takes a block replica and return whether the replica is in partition
        """

        def __init__(self, name, func):
            self.name = name
            self._partitioning = func

        def __call__(self, replica):
            return self._partitioning(replica)

    partitions = {} # name -> Partition
    _partitions_order = [] # list of partitions

    # must be called before any Site is instantiated
    @staticmethod
    def set_partitions(config):
        for name, func in config:
            partition = Site.Partition(name, func)
            Site.partitions[name] = partition
            Site._partitions_order.append(partition)


    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', storage = 0., cpu = 0., status = STAT_UNKNOWN, active = ACT_AVAILABLE):
        self.name = name
        self.host = host
        self.storage_type = storage_type
        self.backend = backend
        self.storage = storage # in TB
        self.cpu = cpu # in kHS06
        self.status = status

        self.active = active

        self.dataset_replicas = set()

        self._block_replicas = set()

        # Each block replica can have multiple owners but will always have one "accounting owner", whose quota the replica counts toward.
        # When the accounting owner disowns the replica, the software must reassign the ownership to another.
        self._partition_quota = [0] * len(Site.partitions) # in TB
        self._occupancy_projected = [0] * len(Site.partitions) # cached sum of block sizes
        self._occupancy_physical = [0] * len(Site.partitions) # cached sum of block replica sizes

    def __str__(self):
        return 'Site %s (host=%s, storage_type=%s, backend=%s, storage=%d, cpu=%f, status=%s, active=%s)' % \
            (self.name, self.host, Site.storage_type_name(self.storage_type), self.backend, self.storage, self.cpu, Site.status_name(self.status), Site.activestate_name(self.active))

    def __repr__(self):
        return 'Site(\'%s\', host=\'%s\', storage_type=%d, backend=\'%s\', storage=%d, cpu=%f, status=%d, active=%d)' % \
            (self.name, self.host, self.storage_type, self.backend, self.storage, self.cpu, self.status, self.active)

    def unlink(self):
        # unlink objects to avoid ref cycles - should be called when this site is absolutely not needed
        while True:
            try:
                replica = self.dataset_replicas.pop()
            except KeyError:
                break

            replica.dataset.replicas.remove(replica)
            replica.dataset = None
            replica.site = None
            for block_replica in replica.block_replicas:
                # need to call remove before clearing the set for size accounting
                self.remove_block_replica(block_replica)

            replica.block_replicas = []

        self._block_replicas.clear()

    def find_dataset_replica(self, dataset):
        # very inefficient operation
        try:
            if type(dataset).__name__ == 'Dataset':
                return next(d for d in list(self.dataset_replicas) if d.dataset == dataset)
            else:
                return next(d for d in list(self.dataset_replicas) if d.dataset.name == dataset)

        except StopIteration:
            return None

    def find_block_replica(self, block):
        try:
            if type(block).__name__ == 'Block':
                return next(b for b in list(self._block_replicas) if b.block == block)
            else:
                return next(b for b in list(self._block_replicas) if b.block.name == block)

        except StopIteration:
            return None

    def add_block_replica(self, replica, partitions = None):
        self._block_replicas.add(replica)

        if partitions is None:
            for ip, partition in enumerate(Site._partitions_order):
                if partition(replica):
                    self._occupancy_projected[ip] += replica.block.size
                    self._occupancy_physical[ip] += replica.size

        else:
            for partition in partitions:
                ip = Site._partitions_order.index(partition)
                self._occupancy_projected[ip] += replica.block.size
                self._occupancy_physical[ip] += replica.size

    def remove_block_replica(self, replica):
        try:
            self._block_replicas.remove(replica)
        except ValueError:
            print replica.site.name, replica.block.dataset.name, replica.block.name
            raise

        for ip, partition in enumerate(Site._partitions_order):
            if partition(replica):
                self._occupancy_projected[ip] -= replica.block.size
                self._occupancy_physical[ip] -= replica.size

    def clear_block_replicas(self):
        self._block_replicas.clear()

        for ip in xrange(len(Site.partitions)):
            self._occupancy_projected[ip] = 0
            self._occupancy_physical[ip] = 0

    def set_block_replicas(self, replicas):
        self._block_replicas.clear()
        self._block_replicas.update(replicas)

        for ip in xrange(len(Site.partitions)):
            partition = Site._partitions_order[ip]
            self._occupancy_projected[ip] = 0
            self._occupancy_physical[ip] = 0
            for replica in self._block_replicas:
                if partition(replica):
                    self._occupancy_projected[ip] += replica.block.size
                    self._occupancy_physical[ip] += replica.size

    def partition_quota(self, partition):
        index = Site._partitions_order.index(partition)

        return self._partition_quota[index]

    def set_partition_quota(self, partition, quota):
        index = Site._partitions_order.index(partition)
        self._partition_quota[index] = quota

    def storage_occupancy(self, partitions = [], physical = True):
        if type(partitions) is not list:
            partitions = [partitions]

        if len(partitions) == 0:
            denom = sum(self._partition_quota)
            if denom == 0:
                return 0.
            else:
                if physical:
                    return sum(self._occupancy_physical) * 1.e-12 / denom
                else:
                    return sum(self._occupancy_projected) * 1.e-12 / denom
        else:
            numer = 0.
            denom = 0.
            for partition in partitions:
                index = Site._partitions_order.index(partition)

                denom += self._partition_quota[index]
                if physical:
                    numer += self._occupancy_physical[index] * 1.e-12
                else:
                    numer += self._occupancy_projected[index] * 1.e-12

            if denom == 0.:
                return 0.
            else:
                return numer / denom

    def quota(self, partitions = []):
        if len(partitions) == 0:
            return sum(self._partition_quota)
        else:
            quota = 0.
            for partition in partitions:
                quota += self.partition_quota(partition)
    
            return quota

