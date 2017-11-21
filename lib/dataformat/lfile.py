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
        return self._directory_id == other._directory_id and self._basename == other._basename and \
            self._block is other._block and self.size == other.size

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
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
        if lfile is None:
            lfile = File(fid, block, self.size)
            block.add_file(lfile)

            return True
        else:
            if lfile is self:
                # identical object -> return False if check is requested
                return not check

            if check and lfile == self:
                return False
            else:
                lfile.copy(self)
                return True

    def delete_from(self, inventory):
        dataset = inventory.datasets[self._block.dataset.name]
        block = dataset.find_block(self._block.name)
        lfile = block.find_file(self.fid())
        block.remove_file(lfile)

    def write_into(self, store, delete = False):
        if delete:
            store.delete_lfile(self)
        else:
            store.save_lfile(self)

    def fid(self):
        return (self._directory_id, self._basename)
