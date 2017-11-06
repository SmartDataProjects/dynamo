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
    
    def fullpath(self):
        return File.directories[self.directory_id] + '/' + self.name
