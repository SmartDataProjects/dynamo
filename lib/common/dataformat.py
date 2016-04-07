import time
import datetime
import collections

class IntegrityError(Exception):
    """Exception to be raised when data integrity error occurs."""

    pass


class ObjectError(Exception):
    """Exception to be raised when object handling rules are violated."""

    pass


class Dataset(object):
    """Represents a dataset."""

    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL
    TYPE_UNKNOWN, TYPE_ALIGN, TYPE_CALIB, TYPE_COSMIC, TYPE_DATA, TYPE_LUMI, TYPE_MC, TYPE_RAW, TYPE_TEST = range(1, 10)
    STAT_UNKNOWN, STAT_DELETED, STAT_DEPRECATED, STAT_INVALID, STAT_PRODUCTION, STAT_VALID, STAT_IGNORED = range(1, 8)

    @staticmethod
    def data_type_name(arg):
        if type(arg) is int:
            data_types = ['UNKNOWN', 'ALIGN', 'CALIB', 'COSMIC', 'DATA', 'LUMI', 'MC', 'RAW', 'TEST']
            return data_types[arg]

        else:
            return arg

    @staticmethod
    def data_type_val(arg):
        if type(arg) is str:
            return eval('Dataset.TYPE_' + arg.upper())

        else:
            return arg

    @staticmethod
    def status_name(arg):
        if type(arg) is int:
            statuses = ['UNKNOWN', 'DELETED', 'DEPRECATED', 'INVALID', 'PRODUCTION', 'VALID', 'IGNORED']
            return statuses[arg]

        else:
            return arg

    @staticmethod
    def status_val(arg):
        if type(arg) is str:
            return eval('Dataset.STAT_' + arg.upper())

        else:
            return arg

    def __init__(self, name, size = -1, num_files = -1, is_open = True, status = STAT_UNKNOWN, on_tape = False, data_type = TYPE_UNKNOWN, software_version = (0, 0, 0, ''), last_update = 0):
        self.name = name
        self.size = size
        self.num_files = num_files
        self.is_open = is_open
        self.status = status
        self.on_tape = on_tape
        self.data_type = data_type
        self.software_version = software_version
        self.last_update = last_update # in UNIX time
        self.blocks = []
        self.replicas = []

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset %s (is_open=%d, status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, #blocks=%d, replicas=%s)' % \
            (self.name, self.is_open, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), len(self.blocks), replica_sites)

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

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))
        return 'Block %s#%s (size=%d, num_files=%d, is_open=%d, replicas=%s)' % (self.dataset.name, self.name, self.size, self.num_files, self.is_open, replica_sites)

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

    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', storage = 0., cpu = 0.):
        self.name = name
        self.host = host
        self.storage_type = storage_type
        self.backend = backend
        self.storage = storage # in TB
        self.cpu = cpu # in kHS06
        self.group_quota = {} # in TB
        self.dataset_replicas = []
        self.block_replicas = []

    def __str__(self):
        return 'Site %s (host=%s, storage_type=%s, backend=%s, storage=%d, cpu=%f)' % \
            (self.name, self.host, Site.storage_type_name(self.storage_type), self.backend, self.storage, self.cpu)

    def find_dataset_replica(self, ds_name):
        try:
            return next(d for d in self.dataset_replicas if d.dataset.name == ds_name)
        except StopIteration:
            return None

    def find_block_replica(self, block_name):
        try:
            return next(b for b in self.block_replicas if b.block.name == block_name)
        except StopIteration:
            return None

    def group_usage(self, group):
        return sum([r.block.size for r in self.block_replicas if r.group == group])

    def storage_occupancy(self, groups = []):
        if type(groups) is not list:
            groups = [groups]

        if len(groups) == 0:
            return sum([r.block.size for r in self.block_replicas]) * 1.e-12 / sum(self.group_quota.values())
        else:
            numer = 0.
            denom = 0.
            for group in groups:
                try:
                    denom += self.group_quota[group]
                except KeyError:
                    continue

                numer += self.group_usage(group) * 1.e-12

            return numer / denom

    def quota(self, groups):
        if type(groups) is not list:
            groups = [groups]

        quota = 0.
        for group in groups:
            try:
                quota += self.group_quota[group]
            except KeyError:
                pass

        return quota

class Group(object):
    """Represents a user group."""

    def __init__(self, name):
        self.name = name


class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    # Access types.
    # Starting from 1 to play better with MySQL.
    ACC_LOCAL, ACC_REMOTE = range(1, 3)
    Access = collections.namedtuple('Access', ['num_accesses', 'cputime'])

    def __init__(self, dataset, site, group = None, is_complete = False, is_partial = False, is_custodial = False):
        self.dataset = dataset
        self.site = site
        self.group = group # None also if owned by multiple groups
        self.is_complete = is_complete # = complete subscription. Can still be partial
        self.is_partial = is_partial
        self.is_custodial = is_custodial
        self.block_replicas = []
        self.accesses = dict([(i, {}) for i in range(1, 3)]) # UTC date -> Accesses

    def is_last_copy(self):
        return len(self.dataset.replicas) == 1

    def size(self):
        if self.is_partial:
            return sum([r.block.size for r in self.block_replicas])
        else:
            return self.dataset.size


class BlockReplica(object):
    """Represents a block replica."""

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False):
        self.block = block
        self.site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial


class DatasetDemand(object):
    """Represents information on dataset demand."""

    def __init__(self, dataset, required_copies = 1, popularity_score = -1.):
        self.dataset = dataset
        self.popularity_score = popularity_score
        self.required_copies = required_copies
        self.locked_blocks = []
