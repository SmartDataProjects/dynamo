from dataformat.exceptions import ObjectError

class Block(object):
    """Smallest data unit for data management."""

    __slots__ = ['_name', '_dataset', 'size', 'num_files', 'is_open', 'replicas', 'files']

    @property
    def name(self):
        return self._name

    @property
    def dataset(self):
        return self._dataset

    @staticmethod
    def translate_name(name_str):
        # block name format: [8]-[4]-[4]-[4]-[12] where [n] is an n-digit hex.
        return int(name_str.replace('-', ''), 16)

    def __init__(self, name, dataset, size = 0, num_files = 0, is_open = False):
        self._name = name
        self._dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open

        self.replicas = set()

        # needs to be a weak set - weakref.WeakSet is only available for py2.7
        self.files = None

    def __str__(self):
        return 'Block %s#%s (size=%d, num_files=%d, is_open=%s)' % (self._dataset.name, self.real_name(), self.size, self.num_files, self.is_open)

    def __repr__(self):
        return 'Block(translate_name(\'%s\'), %s)' % (self.real_name(), repr(self._dataset))

    def __eq__(self, other):
        return self._name == other._name and self._dataset is other._dataset and \
            self.size == other.size and self.num_files == other.num_files and self.is_open == other.is_open

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        self._dataset = other.dataset
        self.size = other.size
        self.num_files = other.num_files
        self.is_open = other.is_open

    def unlinked_clone(self):
        dataset = self._dataset.unlinked_clone()
        return Block(self._name, dataset, self.size, self.num_files, self.is_open)

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._dataset.name]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset.name)

        block = dataset.find_block(self._name)
        if block is None:
            dataset = inventory.datasets[self._dataset.name]
            block = Block(self._name, dataset, self.size, self.num_files, self.is_open)
            dataset.blocks.add(block)

            return True
        else:
            if check and block == self:
                return False
            else:
                block.copy(self)
                return True

    def delete_from(self, inventory):
        # Remove the block from the dataset, and remove all replicas.
        dataset = inventory.datasets[self._dataset.name]
        block = dataset.find_block(self._name, must_find = True)
        dataset.remove_block(block)
        
        for replica in block.replicas:
            replica.site.remove_block_replica(replica)

    def real_name(self):
        full_string = hex(self._name).replace('0x', '')[:-1] # last character is 'L'
        if len(full_string) < 32:
            full_string = '0' * (32 - len(full_string)) + full_string

        return full_string[:8] + '-' + full_string[8:12] + '-' + full_string[12:16] + '-' + full_string[16:20] + '-' + full_string[20:]

    def full_name(self):
        return self._dataset.name + '#' + self.real_name()

    def find_file(self, lfn, must_find = False):
        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.full_name())

        try:
            if type(lfn) is str:
                return next(f for f in self.files if f.lfn == lfn)
            else:
                # can be a tuple (directory_id, basename)
                return next(f for f in self.files if f.fid() == lfn)

        except StopIteration:
            if must_find:
                raise ObjectError('Cannot find file %s', str(lfn))
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
                raise ObjectError('Cannot find replica at %s for %s', site.name, self.full_name())
            else:
                return None

    def add_file(self, lfile):
        # this function can change block_replica.is_complete

        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.full_name())

        self.files.add(lfile)
        self.size += lfile.size
        self.num_files += 1

    def remove_file(self, lfile):
        # this function can change block_replica.is_complete

        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.full_name())

        self.files.remove(lfile)
        self.size -= lfile.size
        self.num_files -= 1

        for replica in self.replicas:
            if replica.files is not None:
                try:
                    replica.files.remove(lfile)
                except ValueError:
                    pass
                else:
                    replica.size -= lfile.size
