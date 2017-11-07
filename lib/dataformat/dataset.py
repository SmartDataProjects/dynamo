import time

from dataformat.exceptions import ObjectError

class Dataset(object):
    """Represents a dataset."""

    __slots__ = ['name', 'size', 'num_files', 'status', 'on_tape',
        'data_type', 'software_version', 'last_update', 'is_open',
        'blocks', 'replicas', 'requests', 'demand']

    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL
    TYPE_UNKNOWN, TYPE_ALIGN, TYPE_CALIB, TYPE_COSMIC, TYPE_DATA, TYPE_LUMI, TYPE_MC, TYPE_RAW, TYPE_TEST = range(1, 10)
    STAT_UNKNOWN, STAT_DELETED, STAT_DEPRECATED, STAT_INVALID, STAT_PRODUCTION, STAT_VALID, STAT_IGNORED = range(1, 8)
    TAPE_NONE, TAPE_FULL, TAPE_PARTIAL = range(3)

    @staticmethod
    def data_type_name(arg):
        if type(arg) is int:
            data_types = ['UNKNOWN', 'ALIGN', 'CALIB', 'COSMIC', 'DATA', 'LUMI', 'MC', 'RAW', 'TEST']
            return data_types[arg - 1]

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
            return statuses[arg - 1]

        else:
            return arg

    @staticmethod
    def status_val(arg):
        if type(arg) is str:
            return eval('Dataset.STAT_' + arg.upper())

        else:
            return arg

    def __init__(self, name, size = 0, num_files = 0, status = STAT_UNKNOWN, on_tape = TAPE_NONE, data_type = TYPE_UNKNOWN, software_version = None, last_update = 0, is_open = True):
        self.name = name
        self.size = size # redundant with sum of block sizes when blocks are loaded
        self.num_files = num_files # redundant with sum of block num_files and len(files)
        self.status = status
        self.on_tape = on_tape
        self.data_type = data_type
        self.software_version = software_version
        self.last_update = last_update # in UNIX time
        self.is_open = is_open

        # "transient" members
        self.blocks = None # is a set when loaded
        self.replicas = None # is a set when loaded
        self.demand = {} # freeform key-value pairs

    def __str__(self):
        if self.replicas is None:
            replica_sites = '?'
        else:
            replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset(\'%s\', size=%d, num_files=%d, status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, is_open=%s, %s blocks, replicas=%s)' % \
            (self.name, self.size, self.num_files, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), str(self.is_open), \
            '?' if self.blocks is None else str(len(self.blocks)), replica_sites)

    def __repr__(self):
        return 'Dataset(\'%s\')' % self.name

    def make_valid(self):
        self.status = Dataset.STAT_VALID
        self.blocks = set()
        self.replicas = set()

    def find_block(self, block_name):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        try:
            return next(b for b in self.blocks if b.name == block_name)
        except StopIteration:
            return None

    def find_file(self, path):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        for block in self.blocks:
            f = block.find_file(path)
            if f is not None:
                return f

        return None

    def find_replica(self, site):
        if self.replicas is None:
            raise ObjectError('Replicas are not loaded for %s' % self.name)

        try:
            if type(site) is str:
                return next(r for r in self.replicas if r.site.name == site)
            else:
                return next(r for r in self.replicas if r.site == site)

        except StopIteration:
            return None

    def remove_block(self, block):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        self.blocks.remove(block)
        self.size -= block.size
        self.num_files -= block.num_files

        if self.replicas is not None:
            for replica in self.replicas:
                block_replica = replica.find_block_replica(block)
                if block_replica is not None:
                    replica.remove_block_replica(block_replica)
