import collections

# Block and BlockReplica implemented as tuples to reduce memory footprint
BlockReplica = collections.namedtuple('BlockReplica', ['block', 'site', 'group', 'is_complete', 'is_custodial', 'size'])

def _BlockReplica_clone(self, **kwd):
    return BlockReplica(
        self.block if 'block' not in kwd else kwd['block'],
        self.site if 'site' not in kwd else kwd['site'],
        self.group if 'group' not in kwd else kwd['group'],
        self.is_complete if 'is_complete' not in kwd else kwd['is_complete'],
        self.is_custodial if 'is_custodial' not in kwd else kwd['is_custodial'],
        self.size if 'size' not in kwd else kwd['size']
    )

BlockReplica.clone = _BlockReplica_clone
