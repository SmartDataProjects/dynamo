from dataformat.exceptions import ObjectError

class BlockReplica(object):
    """Block placement at a site. Holds an attribute 'group' which can be None.
    BlockReplica size can be different from that of the Block."""

    __slots__ = ['_block', '_site', 'group', 'is_complete', 'is_custodial', 'size', 'last_update', 'files']

    @property
    def block(self):
        return self._block

    @property
    def site(self):
        return self._site

    def __init__(self, block, site, group = None, is_complete = False, is_custodial = False, size = -1, last_update = 0):
        self._block = block
        self._site = site
        self.group = group
        self.is_complete = is_complete
        self.is_custodial = is_custodial
        if size < 0:
            self.size = block.size
        else:
            self.size = size
        self.last_update = last_update

        # set of File objects for incomplete replicas
        self.files = None

    def __str__(self):
        return 'BlockReplica %s/%s#%s (group=%s, is_complete=%s, size=%d, last_update=%d)' % \
            (self._site.name, self._block.dataset.name, self._block.real_name(),
                'None' if self.group is None else self.group.name, self.is_complete, self.size, self.last_update)

    def __repr__(self):
        return 'BlockReplica(block=%s, site=%s, group=%s)' % (repr(self._block), repr(self._site), repr(self.group))

    def __eq__(self, other):
        return self._block is other._block and self._site is other._site and self.group is other.group and \
            self.is_complete == other.is_complete and self.is_custodial == other.is_custodial and \
            self.size == other.size and self.last_update == other.last_update

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        self.group = other.group
        self.is_complete = other.is_complete
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update

    def unlinked_clone(self):
        block = self._block.unlinked_clone()
        site = self._site.unlinked_clone()
        if self.group is None:
            group = None
        else:
            group = self.group.unlinked_clone()

        return BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._block.dataset.name]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._block.dataset.name)

        block = dataset.find_block(self._block.name, must_find = True)

        try:
            site = inventory.sites[self._site.name]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site.name)

        if self.group is None:
            group = None
        else:
            try:
                group = inventory.groups[self.group.name]
            except KeyError:
                raise ObjectError('Unknown group %s', self.group.name)

        replica = block.find_replica(site)
        if replica is None:
            replica = BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)
    
            dataset_replica = dataset.find_replica(site, must_find = True)
            dataset_replica.block_replicas.add(replica)
            block.replicas.add(replica)
            site.add_block_replica(replica)

            return True
        else:
            if check and replica == self:
                return False
            else:
                replica.copy(self)
                return True

    def delete_from(self, inventory):
        dataset = inventory.datasets[self._block.dataset.name]
        block = dataset.find_block(self._block.name, must_find = True)
        site = inventory.sites[self._site.name]
        dataset_replica = site.find_dataset_replica(dataset)
        replica = block.find_replica(site, must_find = True)

        dataset_replica.block_replicas.remove(replica)
        block.replicas.remove(replica)
        site.remove_block_replica(replica)
