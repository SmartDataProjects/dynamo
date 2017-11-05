class File(object):

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

    @staticmethod
    def create(path, block, size):
        return File(File.get_basename(path), File.get_directory_id(path), block, size)

    def __init__(self, name, directory_id, block = None, size = 0):
        self.name = name
        self.directory_id = directory_id
        self.block = block
        self.size = size
    
    def fullpath(self):
        return File.directories[self.directory_id] + '/' + self.name
    
    def clone(self, **kwd):
        if 'path' in kwd:
            path = kwd['path']
            name = File.get_basename(path)
            directory_id = File.get_directory_id(path)
        else:
            name = self.name
            directory_id = self.directory_id
    
        return File(
            name,
            directory_id,
            self.block if 'block' not in kwd else kwd['block'],
            self.size if 'size' not in kwd else kwd['size']
        )
