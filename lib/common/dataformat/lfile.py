import collections

File = collections.namedtuple('File', ['name', 'directory_id', 'block', 'size'])

def _File_get_directory_id(path):
    directory = path[:path.rfind('/')]
    try:
        directory_id = File.directory_ids[directory]
    except:
        directory_id = len(File.directories)
        File.directory_ids[directory] = directory_id
        File.directories.append(directory)

    return directory_id

def _File_get_basename(path):
    return path[path.rfind('/') + 1:]

def _File_create(path, block, size):
    return File(File.get_basename(path), File.get_directory_id(path), block, size)

def _File_fullpath(self):
    return File.directories[self.directory_id] + '/' + self.name

def _File_clone(self, **kwd):
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

File.directories = []
File.directory_ids = {}
File.get_directory_id = staticmethod(_File_get_directory_id)
File.get_basename = staticmethod(_File_get_basename)
File.create = staticmethod(_File_create)
File.fullpath = _File_fullpath
File.clone = _File_clone

