class File(object):
    """Represents a file. Atomic unit of data, but not used in data management."""

    __slots__ = ['name', 'directory_id', 'block', 'size']

    directories = []
    directory_ids = {}

    @staticmethod
    def get_directory_id(path):
        directory = path[:path.rfind('/')]
        try:
            directory_id = File.directory_ids[directory]
        except:
            directory_id = len(File.directories)
            File.directory_ids[directory] = directory_id
            File.directories.append(directory)
    
        return directory_id

    @staticmethod
    def get_basename(path):
        return path[path.rfind('/') + 1:]

    def __init__(self, path, block = None, size = 0):
        self.name = File.get_basename(path)
        self.directory_id = File.get_directory_id(path)
        self.block = block
        self.size = size

    def __str__(self):
        return 'File %s (size=%d, block=%s)' % (self.fullpath(), self.size, repr(self.block))

    def __repr__(self):
        return 'File(path=\'%s\', block=%s, size=%d)' % (self.fullpath(), repr(self.block), self.size)

    def copy(self, other):
        self.size = other.size

    def unlinked_clone(self):
        block = self.block.unlinked_clone()
        return File(self.fullpath(), block, self.size)

    def linked_clone(self, inventory):
        dataset = inventory.datasets[self.block.dataset.name]
        block = dataset.find_block(self.block.name)
        lfile = File(self.fullpath(), block, self.size)
        block.add_file(lfile)
        return lfile
    
    def fullpath(self):
        return File.directories[self.directory_id] + '/' + self.name
