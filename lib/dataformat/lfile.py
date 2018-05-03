from exceptions import ObjectError
from block import Block

class File(object):
    """Represents a file. Atomic unit of data, but not used in data management."""

    __slots__ = ['_lfn', '_block', 'id', 'size']

    @property
    def lfn(self):
        return self._lfn

    @property
    def block(self):
        return self._block

    def __init__(self, lfn, block = None, size = 0, fid = 0):
        self._lfn = lfn
        self._block = block
        self.size = size

        self.id = fid

    def __str__(self):
        return 'File %s (block=%s, size=%d, id=%d)' % (self._lfn, self._block_full_name(), self.size, self.id)

    def __repr__(self):
        return 'File(%s,%s,%d,%d)' % (repr(self._lfn), repr(self._block_full_name()), self.size, self.id)

    def __eq__(self, other):
        return self is other or \
            (self._lfn == other._lfn and self._block_full_name() == other._block_full_name() and \
            self.size == other.size)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._block_full_name() != other._block_full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._block_full_name(), self._block_full_name())

        self.id = other.id
        self.size = other.size

    def embed_into(self, inventory, check = False):
        if self._block_name() is None:
            raise ObjectError('Cannot embed into inventory a stray file %s', self._lfn)

        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset_name())

        block = dataset.find_block(self._block_name(), must_find = True)

        lfile = block.find_file(self._lfn, updating = True)
        updated = False
        if lfile is None:
            lfile = File(self._lfn, block, self.size, self.id)
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
            lfile = block.find_file(self._lfn, must_find = True)
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
