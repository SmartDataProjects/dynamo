import time
import copy

from dataformat.exceptions import ObjectError

class Dataset(object):
    """Represents a dataset."""

    __slots__ = ['_name', 'size', 'num_files', 'status', 'on_tape',
        'data_type', 'software_version', 'last_update', 'is_open',
        'blocks', 'replicas', 'requests', 'demand']

    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL
    TYPE_UNKNOWN, TYPE_ALIGN, TYPE_CALIB, TYPE_COSMIC, TYPE_DATA, TYPE_LUMI, TYPE_MC, TYPE_RAW, TYPE_TEST = range(1, 10)
    STAT_UNKNOWN, STAT_DELETED, STAT_DEPRECATED, STAT_INVALID, STAT_PRODUCTION, STAT_VALID, STAT_IGNORED = range(1, 8)
    TAPE_NONE, TAPE_FULL, TAPE_PARTIAL = range(3)

    @property
    def name(self):
        return self._name

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
        self._name = name
        self.size = size # redundant with sum of block sizes when blocks are loaded
        self.num_files = num_files # redundant with sum of block num_files and len(files)
        self.status = status
        self.on_tape = on_tape
        self.data_type = data_type
        self.software_version = software_version
        self.last_update = last_update # in UNIX time
        self.is_open = is_open

        self.blocks = set()
        self.replicas = set()

        # "transient" members
        self.demand = {} # freeform key-value pairs

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset(\'%s\', size=%d, num_files=%d, status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, is_open=%s, %d blocks, replicas=%s)' % \
            (self._name, self.size, self.num_files, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), str(self.is_open), \
            len(self.blocks), replica_sites)

    def __repr__(self):
        return 'Dataset(\'%s\')' % self._name

    def __eq__(self, other):
        return self._name == other._name and self.size == other.size and self.num_files == other.num_files and \
            self.status == other.status and self.on_tape == other.on_tape and self.data_type == other.data_type and \
            self.software_version == other.software_version and self.last_update == other.last_update and self.is_open == other.is_open

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        self.size = other.size
        self.num_files = other.num_files
        self.status = other.status
        self.on_tape = other.on_tape
        self.data_type = other.data_type
        self.software_version = other.software_version
        self.last_update = other.last_update
        self.is_open = other.is_open

        self.demand = copy.deepcopy(other.demand)

    def unlinked_clone(self):
        dataset = Dataset(self._name, self.size, self.num_files, self.status, self.on_tape, self.data_type,
            self.software_version, self.last_update, self.is_open)

        dataset.demand = copy.deepcopy(self.demand)

        return dataset

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._name]
        except KeyError:
            dataset = self.unlinked_clone()
            inventory.datasets.add(dataset)
    
            for block in self.blocks:
                block.embed_into(inventory) # gets added to dataset.blocks

            return True
        else:
            if dataset is self:
                # identical object -> return False if check is requested
                return not check

            if check and dataset == self:
                return False
            else:
                dataset.copy(self)
                return True

    def delete_from(self, inventory):
        # Pop the dataset from the main list, and remove all replicas.
        dataset = inventory.datasets.pop(self._name)

        for replica in dataset.replicas:
            replica.site.remove_dataset_replica(replica)

    def find_block(self, block_name, must_find = False):
        try:
            return next(b for b in self.blocks if b.name == block_name)
        except StopIteration:
            if must_find:
                raise ObjectError('Could not find block %s in %s', block_name, self._name)
            else:
                return None

    def find_file(self, path, must_find = False):
        for block in self.blocks:
            f = block.find_file(path)
            if f is not None:
                return f

        if must_find:
            raise ObjectError('Could not find file %s in %s', path, self._name)
        else:
            return None

    def find_replica(self, site, must_find = False):
        try:
            if type(site) is str:
                return next(r for r in self.replicas if r.site.name == site)
            else:
                return next(r for r in self.replicas if r.site == site)

        except StopIteration:
            if must_find:
                raise ObjectError('Could not find replica on %s of %s', str(site), self._name)
            else:
                return None

    def add_block(self, block):
        self.blocks.add(block)
        self.size += block.size
        self.num_files += block.num_files

    def remove_block(self, block):
        self.blocks.remove(block)
        self.size -= block.size
        self.num_files -= block.num_files

        for replica in self.replicas:
            block_replica = replica.find_block_replica(block)
            if block_replica is not None:
                replica.remove_block_replica(block_replica)
