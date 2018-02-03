from exceptions import ObjectError
from block import Block

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
        if size < 0 and type(block) is not str:
            self.size = block.size
        else:
            self.size = size
        self.last_update = last_update

        # set of File objects for incomplete replicas (not implemented)
        self.files = None

    def __str__(self):
        return 'BlockReplica %s:%s (group=%s, is_complete=%s, size=%d, last_update=%d)' % \
            (self._site_name(), self._block_full_name(),
                self._group_name(), self.is_complete, self.size, self.last_update)

    def __repr__(self):
        return 'BlockReplica(block=%s, site=%s, group=%s)' % (repr(self._block), repr(self._site), repr(self.group))

    def __eq__(self, other):
        return self is other or \
            (self._block_full_name() == other._block_full_name() and self._site_name() == other._site_name() and \
            self._group_name() == other._group_name() and \
            self.is_complete == other.is_complete and self.is_custodial == other.is_custodial and \
            self.size == other.size and self.last_update == other.last_update)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._block_full_name() != other._block_full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._block_full_name(), self._block_full_name())
        if self._site_name() != other._site_name():
            raise ObjectError('Cannot copy a replica at %s into a replica at %s', other._site.name, self._site_name())

        self.group = other.group
        self.is_complete = other.is_complete
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update

    def unlinked_clone(self, attrs = True):
        if attrs:
            return BlockReplica(self._block_full_name(), self._site_name(), self._group_name(), self.is_complete, self.is_custodial, self.size, self.last_update)
        else:
            return BlockReplica(self._block_full_name(), self._site_name(), self._group_name())

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset_name())

        block = dataset.find_block(self._block_name(), must_find = True)

        try:
            site = inventory.sites[self._site_name()]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site_name())

        try:
            group = inventory.groups[self._group_name()]
        except KeyError:
            raise ObjectError('Unknown group %s', self._group_name())

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
            if type(self.group) is str or self.group is None:
                # can happen if self is an unlinked clone
                replica.group = group

            site.update_partitioning(replica)
            updated = True

        if check:
            return replica, updated
        else:
            return replica

    def delete_from(self, inventory):
        try:
            dataset = inventory.datasets[self._dataset_name()]
            block = dataset.find_block(self._block_name(), must_find = True)
            site = inventory.sites[self._site_name()]
            replica = block.find_replica(site, must_find = True)
        except (KeyError, ObjecError):
            return None

        replica.unlink()
        return replica

    def unlink(self, dataset_replica = None, unlink_dataset_replica = True):
        if dataset_replica is None:
            dataset_replica = self._site.find_dataset_replica(self._block._dataset, must_find = True)

        for site_partition in self._site.partitions.itervalues():
            try:
                block_replicas = site_partition.replicas[dataset_replica]
            except KeyError:
                continue

            if block_replicas is None:
                # site_partition contained all block replicas. It will contain all after a deletion.
                continue

            block_replicas.remove(replica)

            if len(block_replicas) == 0:
                site_partition.replicas.pop(dataset_replica)

        dataset_replica.block_replicas.remove(self)
        if unlink_dataset_replica and len(dataset_replica.block_replicas) == 0:
            dataset_replica.unlink()

        self._block.replicas.remove(self)

    def write_into(self, store, delete = False):
        if delete:
            store.delete_blockreplica(self)
        else:
            store.save_blockreplica(self)

    def _block_full_name(self):
        if type(self._block) is str:
            return self._block
        else:
            return self._block.full_name()

    def _block_real_name(self):
        if type(self._block) is str:
            return self._block[self._block.find('#') + 1:]
        else:
            return self._block.real_name()

    def _block_name(self):
        if type(self._block) is str:
            return Block.to_internal_name(self._block_real_name())
        else:
            return self._block.name

    def _dataset_name(self):
        if type(self._block) is str:
            return self._block[:self._block.find('#')]
        else:
            return self._block.dataset.name

    def _site_name(self):
        if type(self._site) is str:
            return self._site
        else:
            return self._site.name

    def _group_name(self):
        if type(self.group) is str or self.group is None:
            return self.group
        else:
            return self.group.name
