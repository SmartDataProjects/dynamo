from dataformat.exceptions import ObjectError

class Block(object):
    """Smallest data unit for data management."""

    __slots__ = ['name', 'dataset', 'size', 'num_files', 'is_open', 'replicas', 'files']

    @staticmethod
    def translate_name(name_str):
        # block name format: [8]-[4]-[4]-[4]-[12] where [n] is an n-digit hex.
        return int(name_str.replace('-', ''), 16)

    def __init__(self, name, dataset, size = 0, num_files = 0, is_open = False):
        self.name = name
        self.dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open

        self.replicas = set()

        # needs to be a weak set - weakref.WeakSet is only available for py2.7
        self.files = None

    def __str__(self):
        return 'Block %s#%s (size=%d, num_files=%d, is_open=%s)' % (self.dataset.name, self.real_name(), self.size, self.num_files, self.is_open)

    def __repr__(self):
        return 'Block(translate_name(\'%s\'), %s)' % (self.real_name(), repr(self.dataset))

    def copy(self, other):
        """Only copy simple member variables."""

        self.dataset = other.dataset
        self.size = other.size
        self.num_files = other.num_files
        self.is_open = other.is_open

    def unlinked_clone(self):
        dataset = self.dataset.unlinked_clone()
        return Block(self.name, dataset, self.size, self.num_files, self.is_open)

    def linked_clone(self, inventory):
        dataset = inventory.datasets[self.dataset.name]
        block = Block(self.name, dataset, self.size, self.num_files, self.is_open)
        dataset.blocks.add(block)

        return block

    def real_name(self):
        full_string = hex(self.name).replace('0x', '')[:-1] # last character is 'L'
        if len(full_string) < 32:
            full_string = '0' * (32 - len(full_string)) + full_string

        return full_string[:8] + '-' + full_string[8:12] + '-' + full_string[12:16] + '-' + full_string[16:20] + '-' + full_string[20:]

    def full_name(self):
        return self.dataset.name + '#' + self.real_name()

    def find_file(self, path):
        if self.files is None:
            raise ObjectError('Files are not loaded for %s' % self.full_name())

        try:
            return next(f for f in self.files if f.fullpath() == path)
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
