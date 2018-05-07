import time
import copy
import threading

from exceptions import ObjectError

class Dataset(object):
    """Represents a dataset."""

    __slots__ = ['_name', 'id', 'status', 'data_type',
        '_software_version_id', 'last_update', 'is_open',
        'blocks', 'replicas', 'attr']

    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL
    _data_types = ['unknown', 'align', 'calib', 'cosmic', 'data', 'lumi', 'mc', 'raw', 'test']
    TYPE_UNKNOWN, TYPE_ALIGN, TYPE_CALIB, TYPE_COSMIC, TYPE_DATA, TYPE_LUMI, TYPE_MC, TYPE_RAW, TYPE_TEST = range(1, len(_data_types) + 1)
    _statuses = ['unknown', 'deleted', 'deprecated', 'invalid', 'production', 'valid', 'ignored']
    STAT_UNKNOWN, STAT_DELETED, STAT_DEPRECATED, STAT_INVALID, STAT_PRODUCTION, STAT_VALID, STAT_IGNORED = range(1, len(_statuses) + 1)

    class SoftwareVersion(object):
        __slots__ = ['id', 'value']
    
        def __init__(self, value, vid = 0):
            self.id = vid
            self.value = value

    _software_versions_byid = []
    _software_versions_byvalue = {}
    _software_version_lock = threading.Lock()

    @staticmethod
    def data_type_name(arg):
        try:
            return Dataset._data_types[arg - 1]
        except:
            return arg

    @staticmethod
    def data_type_val(arg):
        try:
            return eval('Dataset.TYPE_' + arg.upper())
        except:
            return arg

    @staticmethod
    def status_name(arg):
        try:
            return Dataset._statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('Dataset.STAT_' + arg.upper())
        except:
            return arg

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return sum(b.size for b in self.blocks)

    @property
    def num_files(self):
        return sum(b.num_files for b in self.blocks)

    @property
    def files(self):
        all_files = set()
        for block in self.blocks:
            all_files.update(block.files)

        return all_files

    @property
    def software_version(self):
        return Dataset._software_versions_byid[self._software_version_id].value

    @software_version.setter
    def software_version(self, value):
        with Dataset._software_version_lock:
            try:
                version = Dataset._software_versions_byvalue[value]
            except KeyError:
                vid = len(Dataset._software_versions_byid)
                version = Dataset.SoftwareVersion(value, vid = vid)
                Dataset._software_versions_byid.append(version)
                Dataset._software_versions_byvalue[value] = version
    
        self._software_version_id = version.id

    def __init__(self, name, status = STAT_UNKNOWN, data_type = TYPE_UNKNOWN, software_version = None, last_update = 0, is_open = True, did = 0):
        self._name = name
        self.status = Dataset.status_val(status)
        self.data_type = Dataset.data_type_val(data_type)
        self.software_version = software_version
        self.last_update = last_update # in UNIX time
        self.is_open = is_open

        self.id = did

        self.blocks = set()
        self.replicas = set()

        # "transient" members - excluded in __getstate__
        self.attr = {} # freeform key-value pairs

    def __str__(self):
        replica_sites = '[%s]' % (','.join([r.site.name for r in self.replicas]))

        return 'Dataset %s (status=%s, data_type=%s, software_version=%s, last_update=%s, is_open=%s, id=%d, size %d, %d blocks, %d files, replicas %s)' % \
            (self._name, Dataset.status_name(self.status), Dataset.data_type_name(self.data_type), \
            str(self.software_version), time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(self.last_update)), str(self.is_open), \
            self.id, self.size, len(self.blocks), self.num_files, replica_sites)

    def __repr__(self):
        return 'Dataset(%s,\'%s\',\'%s\',%s,%d,%s,%d)' % \
            (repr(self._name), Dataset.status_name(self.status), Dataset.data_type_name(self.data_type), \
            repr(self.software_version), self.last_update, self.is_open, self.id)

    def __eq__(self, other):
        return self is other or \
            (self._name == other._name and self.status == other.status and \
            self.data_type == other.data_type and self._software_version_id == other._software_version_id and \
            self.last_update == other.last_update and self.is_open == other.is_open)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getstate__(self):
        state = dict((s, getattr(self, s)) for s in Dataset.__slots__ if s != 'attr')
        state['attr'] = {}
        return state

    def __setstate__(self, state):
        # Need this function because Dataset does not have __dict__
        for key, value in state.iteritems():
            setattr(self, key, value)

    def copy(self, other):
        self.status = other.status
        self.data_type = other.data_type
        self._software_version_id = other._software_version_id
        self.last_update = other.last_update
        self.is_open = other.is_open

        self.attr = copy.deepcopy(other.attr)

    def embed_into(self, inventory, check = False):
        updated = False

        try:
            dataset = inventory.datasets[self._name]
        except KeyError:
            dataset = Dataset(self._name)
            dataset.copy(self)
            inventory.datasets.add(dataset)
    
            updated = True
        else:
            if check and (dataset is self or dataset == self):
                # identical object -> return False if check is requested
                pass
            else:
                dataset.copy(self)
                updated = True

        if check:
            return dataset, updated
        else:
            return dataset

    def unlink_from(self, inventory):
        try:
            dataset = inventory.datasets.pop(self._name)
        except KeyError:
            return None

        for replica in list(dataset.replicas):
            replica.unlink()
        
        for block in list(dataset.blocks):
            block.unlink()

        return dataset

    def write_into(self, store):
        store.save_dataset(self)

    def delete_from(self, store):
        store.delete_dataset(self)

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
