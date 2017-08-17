import collections

# File and FileReplica implemented as tuples to reduce memory footprint
FileReplica = collections.namedtuple('FileReplica', ['lfile', 'site', 'is_complete', 'size'])

def _FileReplica_clone(self, **kwd):
    return FileReplica(
        self.lfile if 'lfile' not in kwd else kwd['lfile'],
        self.site if 'site' not in kwd else kwd['site'],
        self.is_complete if 'is_complete' not in kwd else kwd['is_complete'],
        self.size if 'size' not in kwd else kwd['size']
    )

FileReplica.clone = _FileReplica_clone
