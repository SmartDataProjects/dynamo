class IntegrityError(Exception):
    """Exception to be raised when data integrity error occurs."""

    pass


class ObjectError(Exception):
    """Exception to be raised when object handling rules are violated."""

    pass


class Dataset(object):
    """Represents a dataset."""

    def __init__(self, name, size = -1, num_files = 0, is_open = False, on_tape = False, is_valid = True):
        self.name = name
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.on_tape = on_tape
        self.is_valid = is_valid
        self.last_accessed = 0
        self.blocks = []
        self.replicas = []

    def find_block(self, block_name):
        try:
            return next(b for b in self.blocks if b.name == block_name)
        except StopIteration:
            return None

    def find_replica(self, site):
        try:
            if type(site) is Site:
                return next(r for r in self.replicas if r.site == site)
            else:
                return next(r for r in self.replicas if r.site.name == site)

        except StopIteration:
            return None


class Block(object):
    """Represents a data block."""

    def __init__(self, name, dataset = None, size = -1, num_files = 0, is_open = False):
        self.name = name
        self.dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.replicas = []

    def find_replica(self, site):
        try:
            if type(site) is Site:
                return next(r for r in self.replicas if r.site == site)
            else:
                return next(r for r in self.replicas if r.site.name == site)

        except StopIteration:
            return None


class Site(object):
    """Represents an SE."""

    TYPE_DISK = 0
    TYPE_MSS = 1
    TYPE_BUFFER = 2
    TYPE_UNKNOWN = 3

    @staticmethod
    def storage_type(arg):
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

        elif type(arg) is int:
            if arg == Site.TYPE_DISK:
                return 'disk'
            elif arg == Site.TYPE_MSS:
                return 'mss'
            elif arg == Site.TYPE_BUFFER:
                return 'buffer'
            elif arg == Site.TYPE_UNKNOWN:
                return 'unknown'

        else:
            raise ObjectError('storage_type argument {arg} is invalid'.format(arg = arg))

    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', capacity = 0, used_total = 0):
        self.name = name
        self.host = host
        self.storage_type = storage_type
        self.backend = backend
        self.capacity = capacity
        self.used_total = used_total
        self.datasets = []
        self.blocks = []

    def find_dataset(self, ds_name):
        try:
            return next(d for d in self.datasets if d.name == ds_name)
        except StopIteration:
            return None

    def find_block(self, block_name):
        try:
            return next(b for b in self.blocks if b.name == block_name)
        except StopIteration:
            return None

    def num_last_copy(self):
        """
        Number of datasets on this site that have no other replicas.
        """

        return sum([1 for d in self.datasets if len(d.replicas) == 1])

    def last_copy_fraction(self):
        return float(self.num_last_copy()) / float(len(self.datasets))

    def occupancy(self):
        return float(self.used_total) / float(self.capacity)


class Group(object):
    """Represents a user group."""

    def __init__(self, name):
        self.name = name


class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    def __init__(self, dataset, site, is_complete = False, is_partial = False, is_custodial = False):
        self.dataset = dataset
        self.site = site
        self.is_complete = is_complete # = complete subscription. Can still be partial
        self.is_partial = is_partial
        self.is_custodial = is_custodial
        self.block_replicas = [] # can be empty for complete datasets if loaded from local inventory

    def is_last_copy(self):
        return len(self.dataset.replicas) == 1

    def size(self):
        if is_partial:
            return sum([r.block.size for r in self.block_replicas])
        else:
            return self.dataset.size


class BlockReplica(object):
    """Represents a block replica."""

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False, time_created = 0, time_updated = 0):
        self.block = block
        self.site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial
        self.time_created = time_created
        self.time_updated = time_updated


class DatasetDemand(object):
    """Represents information on dataset demand."""

    def __init__(self, dataset, popularity_score = -1.):
        self.dataset = dataset
        self.popularity_score = popularity_score
        self.locked_blocks = []
