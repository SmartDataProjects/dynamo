class IntegrityError(Exception):
    """Exception to be raised when data integrity error occurs."""

    pass


class ObjectError(Exception):
    """Exception to be raised when object handling rules are violated."""

    pass


class Dataset(object):
    """Represents a dataset."""

    def __init__(self, name, context, size = -1, num_files = 0, is_open = False, blocks = []):
        if context != 'from_instance()':
            raise ObjectError('Dataset object must be instantiated by Dataset.instance()')

        self.name = name
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.blocks = blocks


class Block(object):
    """Represents a data block."""

    def __init__(self, name, context, dataset = None, size = -1, num_files = 0, is_open = False):
        if context != 'from_instance()':
            raise ObjectError('Block object must be instantiated by Block.instance()')

        self.name = name
        self.dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.replicas = []


class Site(object):
    """Represents an SE."""

    def __init__(self, name, capacity = 0, used_total = 0):
        self.name = name
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

    def __init__(self, block, site, is_custodial = False):
        self.block = block
        self.site = site
        self.is_custodial = is_custodial
