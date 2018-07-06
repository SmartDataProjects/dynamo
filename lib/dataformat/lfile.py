from exceptions import ObjectError
from block import Block

class File(object):
    """Represents a file. Atomic unit of data."""

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
            raise ObjectError('Cannot copy a replica of %s into a replica of %s' % (other._block_full_name(), self._block_full_name()))

        self._copy_no_check(other)

    def embed_into(self, inventory, check = False):
        if self._block_name() is None:
            raise ObjectError('Cannot embed into inventory a stray file %s' % self._lfn)

        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s' % self._dataset_name())

        block = dataset.find_block(self._block_name(), must_find = True)

        if hasattr(inventory, 'has_store'):
            # This is the server-side main inventory which doesn't need a running image of files,
            # so we don't call block.find_file (which triggers an inventory store lookup) but simply
            # return a clone of this file linked to the proper block.
            # Also in this case the function will never be called with check = True
            return File(self._lfn, block, self.size, self.id)

        # At this point (if there is any change) block must have loaded files as a non-volatile set
        lfile = block.find_file(self._lfn)
        updated = False
        if lfile is None:
            lfile = File(self._lfn, block, self.size, self.id)
            block.add_file(lfile) # doesn't change the block attributes

            updated = True
        elif check and (lfile is self or lfile == self):
            # identical object -> return False if check is requested
            pass
        else:
            lfile._copy_no_check(self)
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
            block = dataset.find_block(self._block_name(), must_find = True)
            # At this point (if there is any change) block must have loaded files as a real (non-cache) set
        except (KeyError, ObjectError):
            return None

        if hasattr(inventory, 'has_store'):
            # This is the server-side main inventory which doesn't need a running image of files,
            # so we don't call block.find_file (which triggers an inventory store lookup) but simply
            # return a clone of this file linked to the proper block.
            return File(self._lfn, block, self.size, self.id)

        lfile = block.find_file(self._lfn)
        if lfile is None:
            return None

        lfile.unlink()

        return lfile

    def unlink(self):
        self._block.remove_file(self)

        if BlockReplica._use_file_ids:
            if self.id == 0:
                fid = self.lfn
            else:
                fid = self.id
    
            for replica in self._block.replicas:
                if replica.file_ids is None:
                    # replica was full; block shrunk; replica remains full.
                    continue

                if fid in replica.file_ids:
                    tmplist = list(replica.file_ids)
                    tmplist.remove(fid)
                    replica.file_ids = tuple(tmplist)

            # if not using file ids for BlockReplicas, we have no way to tell if the replica contains this file.

    def write_into(self, store):
        store.save_file(self)

    def delete_from(self, store):
        store.delete_file(self)

    def _block_full_name(self):
        if type(self._block) is str or self._block is None:
            # self._block is the full name
            return self._block
        else:
            return self._block.full_name()

    def _block_real_name(self):
        if type(self._block) is str:
            return Block.to_real_name(Block.from_full_name(self._block)[1])
        elif self._block is None:
            return None
        else:
            return self._block.real_name()

    def _block_name(self):
        if type(self._block) is str:
            return Block.from_full_name(self._block)[1]
        elif self._block is None:
            return None
        else:
            return self._block.name

    def _dataset_name(self):
        if type(self._block) is str:
            return Block.from_full_name(self._block)[0]
        elif self._block is None:
            return None
        else:
            return self._block.dataset.name

    def _copy_no_check(self, other):
        self.size = other.size
