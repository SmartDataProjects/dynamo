class BlockReplica(object):
    __slots__ = ['block', 'site', 'group', 'is_complete', 'is_custodial', 'size', 'last_update']

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False, size = 0, last_update = 0):
        self.block = block
        self.site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial
        self.size = size
        self.last_update = last_update

    def clone(self, **kwd):
        return BlockReplica(
            self.block if 'block' not in kwd else kwd['block'],
            self.site if 'site' not in kwd else kwd['site'],
            self.group if 'group' not in kwd else kwd['group'],
            self.is_complete if 'is_complete' not in kwd else kwd['is_complete'],
            self.is_custodial if 'is_custodial' not in kwd else kwd['is_custodial'],
            self.size if 'size' not in kwd else kwd['size'],
            self.last_update if 'last_update' not in kwd else kwd['last_update']
        )

    def unlink(self):
        # Detach this replica from owning containers but retain references from this replica
    
        dataset_replica = self.block.dataset.find_replica(self.site)
        dataset_replica.block_replicas.remove(self)
    
        self.site.remove_block_replica(self)

    def link(self):
        # Reverse operation of unlink
    
        dataset_replica = self.block.dataset.find_replica(self.site)
        if dataset_replica is None:
            return
    
        dataset_replica.block_replicas.append(self)
    
        self.site.add_block_replica(self)
