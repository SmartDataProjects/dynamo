import time

from block import Block
from lfile import File

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

    def __init__(self, name, status = STAT_UNKNOWN, on_tape = TAPE_NONE, data_type = TYPE_UNKNOWN, software_version = None, last_update = 0, is_open = True):
        self.name = name
        self.status = status
        self.on_tape = on_tape
        self.data_type = data_type
        self.software_version = software_version
        self.last_update = last_update # in UNIX time
        self.is_open = is_open

        # "transient" members
        self.blocks = []
        self.files = set()
        self.replicas = []
        self.requests = []
        self.demand = None

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset(\'%s\', status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, is_open=%s, %d blocks, %d files, replicas=%s)' % \
            (self.name, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), str(self.is_open), len(self.blocks), len(self.files), replica_sites)

    def __repr__(self):
        return 'Dataset(\'%s\', status=%d, on_tape=%d, data_type=%d, software_version=%s, last_update=%d, is_open=%s)' % \
            (self.name, self.status, self.on_tape, self.data_type, str(self.software_version), self.last_update, str(self.is_open))

    def unlink(self):
        # unlink objects to avoid ref cycles - should be called when this dataset is absolutely not needed
        for replica in self.replicas:
            replica.dataset = None
            site = replica.site

            for block_replica in replica.block_replicas:
                site.remove_block_replica(block_replica)

            replica.block_replicas = []
            site.dataset_replicas.remove(replica)

        # by removing the dataset replicas, the block replicas should become deletable (site->blockrep link is cut)
        self.replicas = []
        self.demand = None
        self.files = set()
        self.blocks = []

    def size(self):
        return sum(b.size for b in self.blocks)

    def num_files(self):
        # should change to counting the actual files?
        return sum(b.num_files for b in self.blocks)

    def find_block(self, block):
        try:
            if type(block) is long:
                return next(b for b in self.blocks if b.name == block)
            else:
                return next(b for b in self.blocks if b == block)

        except StopIteration:
            return None

    def find_file(self, lfile):
        try:
            if type(lfile) is str:
                directory_id = File.get_directory_id(lfile)
                name = File.get_basename(lfile)
                return next(f for f in self.files if f.name == name and f.directory_id == directory_id)
            else:
                return next(f for f in self.files if f == lfile)

        except StopIteration:
            return None

    def find_replica(self, site):
        try:
            if type(site) is str:
                return next(r for r in self.replicas if r.site.name == site)
            else:
                return next(r for r in self.replicas if r.site == site)

        except StopIteration:
            return None

    def update_block(self, name, size, num_files, is_open):
        # not catching exception intentionally
        old_block = next(b for b in self.blocks if b.name == name)
        self.blocks.remove(old_block)

        new_block = Block(name, self, size, num_files, is_open)
        self.blocks.append(new_block)

        file_replacements = {}
        for lfile in [f for f in self.files if f.block == old_block]:
            new_lfile = lfile.clone(block = new_block)

            self.files.remove(lfile)
            self.files.add(new_lfile)

            file_replacements[lfile] = new_lfile

        for replica in self.replicas:
            for block_replica in [r for r in replica.block_replicas if r.block == old_block]:
                new_block_replica = block_replica.clone(block = new_block)

                replica.block_replicas.remove(block_replica)
                replica.block_replicas.append(new_block_replica)

                replica.site.remove_block_replica(block_replica)
                replica.site.add_block_replica(new_block_replica)

        return new_block

    def remove_block(self, block):
        self.blocks.remove(block)

        for lfile in [f for f in self.files if f.block == block]:
            self.files.remove(lfile)

        for replica in self.replicas:
            for block_replica in [r for r in replica.block_replicas if r.block == block]:
                replica.block_replicas.remove(block_replica)
                replica.site.remove_block_replica(block_replica)

    def update_file(self, path, size):
        directory_id = File.get_directory_id(path)
        name = File.get_basename(path)
        old_file = next(f for f in self.files if f.name == name and f.directory_id == directory_id)
        self.files.remove(old_file)

        new_file = old_file.clone(size = size)
        self.files.add(new_file)

        return new_file
        
