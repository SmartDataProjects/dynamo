import time

from block import Block
from lfile import File
from datasetreplica import DatasetReplica
from exceptions import ObjectError

class Dataset(object):
    """Represents a dataset."""

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
        self.blocks = None # is a list when loaded
        self.files = None # is a set when loaded
        self.replicas = None # is a list when loaded
        self.requests = []
        self.demand = {} # freeform key-value pairs

    def __str__(self):
        if self.replicas is None:
            replica_sites = '?'
        else:
            replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset(\'%s\', size=%d, num_files=%d, status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, is_open=%s, %s blocks, %s files, replicas=%s)' % \
            (self.name, self.size, self.num_files, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), str(self.is_open), \
            '?' if self.blocks is None else str(len(self.blocks)), '?' if self.files is None else str(len(self.files)), replica_sites)

    def __repr__(self):
        return 'Dataset(\'%s\', size=%d, num_files=%d, status=%d, on_tape=%d, data_type=%d, software_version=%s, last_update=%d, is_open=%s)' % \
            (self.name, self.size, self.num_files, self.status, self.on_tape, self.data_type, str(self.software_version), self.last_update, str(self.is_open))

    def make_valid(self):
        self.status = Dataset.STAT_VALID
        self.blocks = []
        self.files = set()
        self.replicas = []

    def unlink(self):
        # unlink objects to avoid ref cycles - should be called when this dataset is absolutely not needed

        if self.replicas is not None:
            for replica in self.replicas:
                replica.dataset = None
                site = replica.site
    
                for block_replica in replica.block_replicas:
                    site.remove_block_replica(block_replica)
    
                replica.block_replicas = []
                site.dataset_replicas.remove(replica)

        # by removing the dataset replicas, the block replicas should become deletable (site->blockrep link is cut)
        self.replicas = None
        self.demand = {}
        self.files = None
        self.blocks = None

    def find_block(self, block):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        try:
            if type(block).__name__ == 'Block':
                return next(b for b in self.blocks if b == block)
            else:
                return next(b for b in self.blocks if b.name == block)

        except StopIteration:
            return None

    def find_file(self, lfile):
        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.name)

        try:
            if type(lfile).__name__ == 'File':
                return next(f for f in self.files if f == lfile)
            else:
                directory_id = File.get_directory_id(lfile)
                name = File.get_basename(lfile)
                return next(f for f in self.files if f.name == name and f.directory_id == directory_id)

        except StopIteration:
            return None

    def find_replica(self, site):
        if self.replicas is None:
            return None

        try:
            if type(site).__name__ == 'Site':
                return next(r for r in self.replicas if r.site == site)
            else:
                return next(r for r in self.replicas if r.site.name == site)

        except StopIteration:
            return None

    def get_replica(self, site):
        # create and link a replica if one does not exist
        # returns (replica, created)

        replica = self.find_replica(site)

        if replica is None:
            replica = DatasetReplica(self, site)
            self.replicas.append(replica)
            site.dataset_replicas.add(replica)

            return replica, True
        else:
            return replica, False

    def update_block(self, name, size, num_files, is_open):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        # not catching exception intentionally
        old_block = next(b for b in self.blocks if b.name == name)
        self.blocks.remove(old_block)

        new_block = Block(name, self, size, num_files, is_open)
        self.blocks.append(new_block)

        self.size += new_block.size - old_block.size
        self.num_files += new_block.num_files - old_block.num_files

        if self.files is not None:
            for lfile in [f for f in self.files if f.block == old_block]:
                new_lfile = lfile.clone(block = new_block)
    
                self.files.remove(lfile)
                self.files.add(new_lfile)

        if self.replicas is not None:
            for replica in self.replicas:
                for block_replica in [r for r in replica.block_replicas if r.block == old_block]:
                    new_block_replica = block_replica.clone(block = new_block)
    
                    replica.block_replicas.remove(block_replica)
                    replica.block_replicas.append(new_block_replica)
    
                    replica.site.remove_block_replica(block_replica)
                    replica.site.add_block_replica(new_block_replica)

        return new_block

    def remove_block(self, block):
        if self.blocks is None:
            raise ObjectError('Blocks are not loaded for %s' % self.name)

        self.blocks.remove(block)
        self.size -= block.size
        self.num_files -= block.num_files

        if self.files is not None:
            for lfile in [f for f in self.files if f.block == block]:
                self.files.remove(lfile)

        if self.replicas is not None:
            for replica in self.replicas:
                for block_replica in [r for r in replica.block_replicas if r.block == block]:
                    replica.block_replicas.remove(block_replica)
                    replica.site.remove_block_replica(block_replica)

    def update_file(self, path, size):
        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.name)

        directory_id = File.get_directory_id(path)
        name = File.get_basename(path)
        old_file = next(f for f in self.files if f.name == name and f.directory_id == directory_id)
        self.files.remove(old_file)

        new_file = old_file.clone(size = size)
        self.files.add(new_file)

        return new_file

