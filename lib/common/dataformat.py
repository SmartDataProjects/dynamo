class IntegrityError(Exception):
    """Exception to be raised when data integrity error occurs."""

    pass


class ObjectError(Exception):
    """Exception to be raised when object handling rules are violated."""

    pass


class Dataset(object):
    """Represents a dataset."""

    def __init__(self, name, size = -1, num_files = 0, is_open = False):
        self.name = name
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.blocks = []
        self.replicas = []


class Block(object):
    """Represents a data block."""

    def __init__(self, name, dataset = None, size = -1, num_files = 0, is_open = False):
        self.name = name
        self.dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.replicas = []


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
        self.backend = ''
        self.capacity = capacity
        self.used_total = used_total
        self.datasets = []
        self.blocks = []


class Group(object):
    """Represents a user group."""

    cached_groups = {}

    def __init__(self, name, group_id = 0):
        self.group_id = group_id
        self.name = name


class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    def __init__(self, dataset, site, is_partial = False, is_custodial = False):
        self.dataset = dataset
        self.site = site
        self.is_partial = is_partial
        self.is_custodial = is_custodial


class BlockReplica(object):
    """Represents a block replica."""

    def __init__(self, block, site, is_custodial = False, time_created = 0, time_updated = 0):
        self.block = block
        self.site = site
        self.is_custodial = is_custodial
        self.time_created = time_created
        self.time_updated = time_updated
