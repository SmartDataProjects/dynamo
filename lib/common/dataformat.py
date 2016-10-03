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
        self.replicas = []
        self.requests = []
        self.demand = None

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset(\'%s\', status=%s, on_tape=%d, data_type=%s, software_version=%s, last_update=%s, is_open=%s, blocks=%s, replicas=%s)' % \
            (self.name, Dataset.status_name(self.status), self.on_tape, Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_update)), str(self.is_open), str([b.real_name() for b in self.blocks]), replica_sites)

    def __repr__(self):
        return 'Dataset(\'%s\', status=%d, on_tape=%d, data_type=%d, software_version=%s, last_update=%d, is_open=%s)' % \
            (self.name, self.status, self.on_tape, self.data_type, str(self.software_version), self.last_update, str(self.is_open))

    def unlink(self):
        # unlink objects to avoid ref cycles - should be called when this dataset is absolutely not needed
        sites = []
        for replica in self.replicas:
            replica.dataset = None
            site = replica.site

            sites.append(site)
            for block_replica in replica.block_replicas:
                site.remove_block_replica(block_replica)
                
            site.dataset_replicas.remove(replica)

        # by removing the dataset replicas, the block replicas should become deletable (site->blockrep link is cut)
        self.replicas = []
        self.demand = None
        self.blocks = []

    def size(self):
        return sum(b.size for b in self.blocks)

    def num_files(self):
        return sum(b.num_files for b in self.blocks)

    def find_block(self, block):
        try:
            if type(block) is Block:
                return next(b for b in self.blocks if b == block)
            else:
                return next(b for b in self.blocks if b.name == block)

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

    def update_block(self, name, size, num_files, is_open):
        # not catching exception intentionally
        old_block = next(b for b in self.blocks if b.name == name)
        self.blocks.remove(old_block)

        new_block = Block(name, self, size, num_files, is_open)
        self.blocks.append(new_block)

        for replica in self.replicas:
            site = replica.site
            for block_replica in list(replica.block_replicas):
                if block_replica.block == old_block:
                    new_block_replica = block_replica.clone(block = new_block)

                    replica.block_replicas.remove(block_replica)
                    replica.block_replicas.append(new_block_replica)

                    site.remove_block_replica(block_replica)
                    site.add_block_replica(new_block_replica)

        return new_block

    def remove_block(self, block):
        self.blocks.remove(block)

        for replica in self.replicas:
            site = replica.site
            for block_replica in list(replica.block_replicas):
                if block_replica.block == block:
                    replica.block_replicas.remove(block_replica)
                    site.remove_block_replica(block_replica)


# Block and BlockReplica implemented as tuples to reduce memory footprint
Block = collections.namedtuple('Block', ['name', 'dataset', 'size', 'num_files', 'is_open'])

def _Block_translate_name(name_str):
    # block name format: [8]-[4]-[4]-[4]-[12] where [n] is an n-digit hex.
    return int(name_str.replace('-', ''), 16)

def _Block___str__(self):
    return 'Block %s#%s (size=%d, num_files=%d, is_open=%s)' % (self.dataset.name, self.real_name(), self.size, self.num_files, self.is_open)

def _Block_real_name(self):
    full_string = hex(self.name).replace('0x', '')[:-1] # last character is 'L'
    if len(full_string) < 32:
        full_string = '0' * (32 - len(full_string)) + full_string

    return full_string[:8] + '-' + full_string[8:12] + '-' + full_string[12:16] + '-' + full_string[16:20] + '-' + full_string[20:]

def _Block_find_replica(self, site):
    try:
        if type(site) is Site:
            return next(r for r in self.replicas if r.site == site)
        else:
            return next(r for r in self.replicas if r.site.name == site)

    except StopIteration:
        return None

def _Block_clone(self, dataset = None, size = None, num_files = None, is_open = None):
    return Block(
        self.name,
        self.dataset if dataset is None else dataset,
        self.size if size is None else size,
        self.num_files if num_files is None else num_files,
        self.is_open if is_open is None else is_open
    )

Block.translate_name = staticmethod(_Block_translate_name)
Block.__str__ = _Block___str__
Block.real_name = _Block_real_name
Block.find_replica = _Block_find_replica
Block.clone = _Block_clone


class Site(object):
    """Represents an SE."""

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
        """

        def __init__(self, name, func):
            self.name = name
            self._partitioning = func

        def __call__(self, replica):
            return self._partitioning(replica)

    partitions = {}
    _partitions_order = []

    @staticmethod
    def add_partition(name, func):
        index = len(Site._partitions)
        partition = Partition(name, func)
        Site.partitions[name] = partition
        Site._partitions_order.append(partition)

    @staticmethod
    def clear_partitions():
        Site.partitions = {}
        Site._partitions_order = []


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
                self.remove_block_replica(block_replica)

            replica.block_replicas = []

        self._block_replicas.clear()

    def find_dataset_replica(self, dataset):
        # very inefficient operation
        try:
            if type(dataset) is Dataset:
                return next(d for d in list(self.dataset_replicas) if d.dataset == dataset)
            else:
                return next(d for d in list(self.dataset_replicas) if d.dataset.name == dataset)

        except StopIteration:
            return None

    def find_block_replica(self, block):
        try:
            if type(block) is Block:
                return next(b for b in list(self._block_replicas) if b.block == block)
            else:
                return next(b for b in list(self._block_replicas) if b.block.name == block)

        except StopIteration:
            return None

    def add_block_replica(self, replica):
        self._block_replicas.add(replica)

        ip = 0
        while ip != len(Site.partitions):
            if Site._partitions_order[ip](replica):
                self._occupancy_projected[ip] += replica.block.size
                self._occupancy_physical[ip] += replica.size

            ip += 1

    def remove_block_replica(self, replica):
        try:
            self._block_replicas.remove(replica)
        except ValueError:
            print replica.site.name, replica.block.dataset.name, replica.block.name
            raise

        ip = 0
        while ip != len(Site.partitions):
            if Site._partitions_order[ip](replica):
                self._occupancy_projected[ip] -= replica.block.size
                self._occupancy_physical[ip] -= replica.size

            ip += 1

    def clear_block_replicas(self):
        self._block_replicas.clear()

        ip = 0
        while ip != len(Site.partitions):
            self._occupancy_projected[ip] = 0
            self._occupancy_physical[ip] = 0
            ip += 1

    def set_block_replicas(self, replicas):
        self._block_replicas.clear()
        self._block_replicas.update(replicas)

        self.compute_occupancy()

    def partition_quota(self, partition):
        index = Site._partitions_order.index(partition)

        return self._partition_quota[index]

    def set_partition_quota(self, partition, quota):
        index = Site._partitions_order.index(partition)
        self._partition_quota[index] = quota

    def compute_occupancy(self):
        ip = 0
        while ip != len(Site.partitions):
            partition = Site._partitions_order[ip]
            self._occupancy_projected[ip] = 0
            self._occupancy_physical[ip] = 0
            for replica in self._block_replicas:
                if partition(replica):
                    self._occupancy_projected[ip] += replica.block.size
                    self._occupancy_physical[ip] += replica.size

            ip += 1

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


class Group(object):
    """
    Represents a user group.
    olevel: ownership level: Dataset or Block
    """

    def __init__(self, name, olevel = Dataset):
        self.name = name
        self.olevel = olevel

    def __str__(self):
        return 'Group %s (olevel=%s)' % (self.name, self.olevel.__name__)

    def __repr__(self):
        return 'Group(\'%s\', %s)' % (self.name, self.olevel.__name__)


class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    # Access types.
    # Starting from 1 to play better with MySQL.
    ACC_LOCAL, ACC_REMOTE = range(1, 3)
    Access = collections.namedtuple('Access', ['num_accesses', 'cputime'])

    def __init__(self, dataset, site, group = None, is_complete = False, is_custodial = False, last_block_created = 0):
        self.dataset = dataset
        self.site = site
        self.group = group # None also if owned by multiple groups
        self.is_complete = is_complete # = complete subscription. Can still be partial
        self.is_custodial = is_custodial
        self.last_block_created = last_block_created
        self.block_replicas = []
        self.accesses = {DatasetReplica.ACC_LOCAL: {}, DatasetReplica.ACC_REMOTE: {}} # UTC date -> Accesses

    def unlink(self):
        self.dataset.replicas.remove(self)
        self.dataset = None

        self.site.dataset_replicas.remove(self)

        for block_replica in self.block_replicas:
            self.site.remove_block_replica(block_replica)

        self.block_replicas = []
        self.site = None

    def __str__(self):
        return 'DatasetReplica {site}:{dataset} (group={group}, is_complete={is_complete}, is_custodial={is_custodial},' \
            ' block_replicas={block_replicas}, #accesses[LOCAL]={num_local_accesses}, #accesses[REMOTE]={num_remote_accesses})'.format(
                site = self.site.name, dataset = self.dataset.name, group = self.group.name if self.group is not None else None, is_complete = self.is_complete,
                is_custodial = self.is_custodial,
                block_replicas = str(self.block_replicas), num_local_accesses = len(self.accesses[DatasetReplica.ACC_LOCAL]),
                num_remote_accesses = len(self.accesses[DatasetReplica.ACC_REMOTE]))

    def __repr__(self):
        rep = 'DatasetReplica(%s,\n' % repr(self.dataset)
        rep += '    %s,\n' % repr(self.site)
        rep += '    group=%s,\n' % repr(self.group)
        rep += '    is_complete=%s,\n' % str(self.is_complete)
        rep += '    is_custodial=%s,\n' % str(self.is_custodial)
        rep += '    last_block_created=%d)' % self.last_block_created

        return rep

    def clone(self, block_replicas = True): # Create a detached clone. Detached in the sense that it is not linked from dataset or site.
        replica = DatasetReplica(dataset = self.dataset, site = self.site, group = self.group, is_complete = self.is_complete, is_custodial = self.is_custodial, last_block_created = self.last_block_created)

        if block_replicas:
            for brep in self.block_replicas:
                replica.block_replicas.append(brep.clone())

        return replica

    def is_last_copy(self):
        return len(self.dataset.replicas) == 1 and self.dataset.replicas[0] == self

    def is_partial(self):
        return self.is_complete and len(self.block_replicas) != len(self.dataset.blocks)

    def is_full(self):
        return self.is_complete and len(self.block_replicas) == len(self.dataset.blocks)

    def size(self, groups = None, physical = True):
        if groups is None:
            if self.is_full():
                return self.dataset.size()
            else:
                if physical:
                    return sum([r.size for r in self.block_replicas])
                else:
                    return sum([r.block.size for r in self.block_replicas])

        elif type(groups) is Group:
            if physical:
                return sum([r.size for r in self.block_replicas if r.group == groups])
            else:
                return sum([r.block.size for r in self.block_replicas if r.group == groups])

        elif type(groups) is list:
            if physical:
                return sum([r.size for r in self.block_replicas if r.group in groups])
            else:
                return sum([r.block.size for r in self.block_replicas if r.group in groups])

    def effective_owner(self):
        if self.group:
            return self.group

        if len(self.block_replicas) == 0:
            return None

        # simple majority
        counts = collections.defaultdict(int)
        for br in self.block_replicas:
            if br.group is not None:
                counts[br.group] += 1

        if len(counts) == 0:
            return None
        elif len(counts) == 1:
            return counts.keys()[0]

        order = sorted(counts.items(), key = lambda (g, c): c, reverse = True)

        if order[0][1] > order[1][1]:
            return order[0][0]
        else:
            # if tied, find alphanumerically first group
            return min([g for g, c in order if c == order[0][1]], key = lambda g: g.name)

    def find_block_replica(self, block):
        try:
            if type(block) is Block:
                return next(b for b in self.block_replicas if b.block == block)
            else:
                return next(b for b in self.block_replicas if b.block.name == block)

        except StopIteration:
            return None

    def last_access(self):
        try:
            last_datetime = max(replica.accesses[DatasetReplica.ACC_LOCAL].keys() + replica.accesses[DatasetReplica.ACC_REMOTE].keys())
        except:
            return 0

        return time.mktime(last_datetime.utctimetuple())


# Block and BlockReplica implemented as tuples to reduce memory footprint
BlockReplica = collections.namedtuple('BlockReplica', ['block', 'site', 'group', 'is_complete', 'is_custodial', 'size'])

def _BlockReplica_clone(self, block = None, site = None, group = None, is_complete = None, is_custodial = None, size = None):
    return BlockReplica(
        self.block if block is None else block,
        self.site if site is None else site,
        self.group if group is None else group,
        self.is_complete if is_complete is None else is_complete,
        self.is_custodial if is_custodial is None else is_custodial,
        self.size if size is None else size
    )

BlockReplica.clone = _BlockReplica_clone


class DatasetDemand(object):
    """Represents information on dataset demand."""

    def __init__(self, request_weight = -1., global_usage_rank = 0):
        self.request_weight = request_weight
        self.global_usage_rank = global_usage_rank
        self.locked_blocks = []


class DatasetRequest(object):
    """Represents a request to a dataset in the job queue"""

    def __init__(self, job_id, queue_time = 0, completion_time = 0, nodes_total = 0, nodes_done = 0, nodes_failed = 0, nodes_queued = 0):
        # queue_time & completion_time are unix timestamps in memory
        self.job_id = job_id
        self.queue_time = queue_time
        self.completion_time = completion_time
        self.nodes_total = nodes_total
        self.nodes_done = nodes_done
        self.nodes_failed = nodes_failed
        self.nodes_queued = nodes_queued

    def update(self, other):
        self.queue_time = other.queue_time
        self.completion_time = other.completion_time
        self.nodes_total = other.nodes_total
        self.nodes_done = other.nodes_done
        self.nodes_failed = other.nodes_failed
        self.nodes_queued = other.nodes_queued


class HistoryRecord(object):
    """Represents a transaction history record."""

    # operation types
    OP_COPY, OP_DELETE = range(2)

    CopiedReplica = collections.namedtuple('CopiedReplica', ['dataset_name'])
    DeletedReplica = collections.namedtuple('DeletedReplica', ['dataset_name'])

    def __init__(self, operation_type, operation_id, site_name, timestamp = 0, approved = False, size = 0, completed = False, last_update = 0):
        self.operation_type = operation_type
        self.operation_id = operation_id
        self.site_name = site_name
        self.timestamp = timestamp
        self.approved = bool(approved)
        self.size = size
        self.completed = completed
        self.replicas = []

    def __del__(self):
        del self.replicas
