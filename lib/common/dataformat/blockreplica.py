class BlockReplica(object):
    """Block placement at a site. Holds an attribute 'group' which can be None.
    BlockReplica size can be different from that of the Block."""

    __slots__ = ['block', 'site', 'group', 'is_complete', 'is_custodial', 'size', 'last_update', 'files']

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False, size = -1, last_update = 0):
        self.block = block
        self.site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial
        if size < 0:
            self.size = block.size
        else:
            self.size = size
        self.last_update = last_update

        # list of File objects for incomplete replicas
        self.files = None

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
