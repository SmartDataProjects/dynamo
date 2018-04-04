from exceptions import ObjectError
from block import Block

class File(object):
    """Represents a file. Atomic unit of data, but not used in data management."""

    __slots__ = ['_directory_id', '_basename', '_block', 'id', 'size']

    directories = []
    directory_ids = {}

    @property
    def lfn(self):
        return File.directories[self._directory_id] + '/' + self._basename

    @property
    def block(self):
        return self._block

    @staticmethod
    def get_directory_id(lfn):
        directory = lfn[:lfn.rfind('/')]
        try:
            directory_id = File.directory_ids[directory]
        except:
            directory_id = len(File.directories)
            File.directory_ids[directory] = directory_id
            File.directories.append(directory)
    
        return directory_id

    @staticmethod
    def get_basename(lfn):
        return lfn[lfn.rfind('/') + 1:]

    def __init__(self, lfn, block = None, size = 0, fid = 0):
        if type(lfn) is str:
            self._directory_id = File.get_directory_id(lfn)
            self._basename = File.get_basename(lfn)
        else:
            # can pass (directory_id, basename)
            self._directory_id = lfn[0]
            self._basename = lfn[1]

        self._block = block
        self.size = size

        self.id = fid

    def __str__(self):
        return 'File %s (block=%s, size=%d, id=%d)' % (self.lfn, self._block_full_name(), self.size, self.id)

    def __repr__(self):
        return 'File(\'%s\',\'%s\',%d,%d)' % (self.lfn, self._block_full_name(), self.size, self.id)

    def __eq__(self, other):
        return self is other or \
            (self._directory_id == other._directory_id and self._basename == other._basename and \
            self._block_full_name() == other._block_full_name() and self.size == other.size)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getstate__(self):
        # if __setstate__ is given, __getstate__ can choose to export data in any format
        return (self.lfn, self._block, self.size, self.id)

    def __setstate__(self, state):
        self._directory_id = File.get_directory_id(state[0])
        self._basename = File.get_basename(state[0])
        self._block = state[1]
        self.size = state[2]
        self.id = state[3]

    def copy(self, other):
        if self._block_full_name() != other._block_full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._block_full_name(), self._block_full_name())

        self.id = other.id
        self.size = other.size

    def embed_into(self, inventory, check = False):
        if self._block_name() is None:
            raise ObjectError('Cannot embed into inventory a stray file %s', self.lfn)

        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset_name())

        block = dataset.find_block(self._block_name(), must_find = True)

        fid = self.fid()

        lfile = block.find_file(fid)
        updated = False
        if lfile is None:
            lfile = File(fid, block, self.size, self.id)
            block.files.add(lfile) # not add_file - block has to be updated by itself

            updated = True
        elif check and (lfile is self or lfile == self):
            # identical object -> return False if check is requested
            pass
        else:
            lfile.copy(self)
            updated = True

        if check:
            return lfile, updated
        else:
            return lfile

    def unlink_from(self, inventory):
        if self._block_name() is None:
            return None

        try:
            dataset = inventory.datasets[self._dataset_name()]
            block = dataset.find_block(self._block_name())
            lfile = block.find_file(self.fid(), must_find = True)
        except (KeyError, ObjectError):
            return None

        lfile.unlink()
        return lfile

    def unlink(self, files = None):
        if files is None:
            files = self._block.files

        files.remove(self)

        self._block.size -= self.size
        self._block.num_files -= 1

    def write_into(self, store):
        store.save_file(self)

    def delete_from(self, store):
        store.delete_file(self)

    def fid(self):
        return (self._directory_id, self._basename)

    def _block_full_name(self):
        if type(self._block) is str or self._block is None:
            return self._block
        else:
            return self._block.full_name()

    def _block_real_name(self):
        if type(self._block) is str:
            return self._block[self._block.find('#') + 1:]
        elif self._block is None:
            return None
        else:
            return self._block.real_name()

    def _block_name(self):
        if type(self._block) is str:
            return Block.to_internal_name(self._block_real_name())
        elif self._block is None:
            return None
        else:
            return self._block.name

    def _dataset_name(self):
        if type(self._block) is str:
            return self._block[:self._block.find('#')]
        elif self._block is None:
            return None
        else:
            return self._block.dataset.name
