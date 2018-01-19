class File(object):
    """Represents a file. Atomic unit of data, but not used in data management."""

    __slots__ = ['_directory_id', '_basename', '_block', 'size']

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

    def __init__(self, lfn, block = None, size = 0):
        if type(lfn) is str:
            self._directory_id = File.get_directory_id(lfn)
            self._basename = File.get_basename(lfn)
        else:
            # can pass (directory_id, basename)
            self._directory_id = lfn[0]
            self._basename = lfn[1]

        self._block = block
        self.size = size

    def __str__(self):
        return 'File %s (block=%s, size=%d)' % (self.lfn, repr(self._block), self.size)

    def __repr__(self):
        return 'File(lfn=\'%s\', block=%s, size=%d)' % (self.lfn, repr(self._block), self.size)

    def __eq__(self, other):
        return self is other or \
            (self._directory_id == other._directory_id and self._basename == other._basename and \
            self._block.full_name() == other._block.full_name() and self.size == other.size)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getstate__(self):
        # if __setstate__ is given, __getstate__ can choose to export data in any format
        return (self.lfn, self._block, self.size)

    def __setstate__(self, state):
        self._directory_id = File.get_directory_id(state[0])
        self._basename = File.get_basename(state[0])
        self._block = state[1]
        self.size = state[2]

    def copy(self, other):
        if self._block.full_name() != other._block.full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._block.full_name(), self._block.full_name())

        self.size = other.size

    def unlinked_clone(self):
        block = self._block.unlinked_clone()
        return File(self.lfn, block, self.size)

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._block.dataset.name]
        except KeyError:
            raise SelfectError('Unknown dataset %s', self._block.dataset.name)

        block = dataset.find_block(self._block.name, must_find = True)

        fid = self.fid()

        lfile = block.find_file(fid)
        updated = False
        if lfile is None:
            lfile = File(fid, block, self.size)
            block.add_file(lfile)

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

    def delete_from(self, inventory):
        dataset = inventory.datasets[self._block.dataset.name]
        block = dataset.find_block(self._block.name)
        lfile = block.find_file(self.fid())
        block.remove_file(lfile)

    def write_into(self, store, delete = False):
        if delete:
            store.delete_file(self)
        else:
            store.save_file(self)

    def fid(self):
        return (self._directory_id, self._basename)
