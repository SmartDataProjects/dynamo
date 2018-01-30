from exceptions import ObjectError

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

    def __init__(self, block, site, group, is_complete = False, is_custodial = False, size = -1, last_update = 0):
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

        # set of File objects for incomplete replicas (not implemented)
        self.files = None

    def __str__(self):
        return 'BlockReplica %s:%s (group=%s, is_complete=%s, size=%d, last_update=%d)' % \
            (self._site.name, self._block.full_name(),
                self.group.name, self.is_complete, self.size, self.last_update)

    def __repr__(self):
        return 'BlockReplica(block=%s, site=%s, group=%s)' % (repr(self._block), repr(self._site), repr(self.group))

    def __eq__(self, other):
        return self is other or \
            (self._block.full_name() == other._block.full_name() and self._site.name == other._site.name and \
            self.group.name == other.group.name and \
            self.is_complete == other.is_complete and self.is_custodial == other.is_custodial and \
            self.size == other.size and self.last_update == other.last_update)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._block.full_name() != other._block.full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._block.full_name(), self._block.full_name())
        if self._site.name != other._site.name:
            raise ObjectError('Cannot copy a replica at %s into a replica at %s', other._site.name, self._site.name)

        self.group = other.group
        self.is_complete = other.is_complete
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update

    def unlinked_clone(self, attrs = True):
        block = self._block.unlinked_clone(attrs = False)
        site = self._site.unlinked_clone(attrs = False)
        group = self.group.unlinked_clone(attrs = False)
        if attrs:
            return BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)
        else:
            return BlockReplica(block, site, group)

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

        try:
            group = inventory.groups[self.group.name]
        except KeyError:
            raise ObjectError('Unknown group %s', self.group.name)

        replica = block.find_replica(site)
        updated = False
        if replica is None:
            replica = BlockReplica(block, site, group, self.is_complete, self.is_custodial, self.size, self.last_update)
    
            dataset_replica = dataset.find_replica(site, must_find = True)
            dataset_replica.block_replicas.add(replica)
            block.replicas.add(replica)
            site.add_block_replica(replica)

            updated = True
        elif check and (replica is self or replica == self):
            # identical object -> return False if check is requested
            pass
        else:
            replica.copy(self)
            site.update_partitioning(replica)
            updated = True

        if check:
            return replica, updated
        else:
            return replica

    def delete_from(self, inventory):
        dataset = inventory.datasets[self._block.dataset.name]
        block = dataset.find_block(self._block.name, must_find = True)
        site = inventory.sites[self._site.name]
        replica = block.find_replica(site, must_find = True)

        return replica._unlink()

    def _unlink(self, dataset_replica = None):
        if dataset_replica is None:
            dataset_replica = self._site.find_dataset_replica(self._block._dataset)

        self._site._remove_block_replica(self, dataset_replica, local = False)

        dataset_replica.block_replicas.remove(self)
        self._block.replicas.remove(self)

        return [self]

    def write_into(self, store, delete = False):
        if delete:
            store.delete_blockreplica(self)
        else:
            store.save_blockreplica(self)
