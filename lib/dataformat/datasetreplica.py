from exceptions import ObjectError

class DatasetReplica(object):
    """Represents a dataset replica. Just a container for block replicas."""

    __slots__ = ['_dataset', '_site', 'block_replicas']

    @property
    def dataset(self):
        return self._dataset

    @property
    def site(self):
        return self._site

    def __init__(self, dataset, site):
        self._dataset = dataset
        self._site = site
        self.block_replicas = set()

    def __str__(self):
        return 'DatasetReplica %s:%s (%d block_replicas)' % \
            (self._site_name(), self._dataset_name(), len(self.block_replicas))

    def __repr__(self):
        return 'DatasetReplica(\'%s\',\'%s\')' % (self._dataset_name(), self._site_name())

    def __eq__(self, other):
        return self is other or (self._dataset_name() == other._dataset_name() and self._site_name() == other._site_name())

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._dataset_name() != other._dataset_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._dataset_name(), self._dataset_name())
        if self._site_name() != other._site_name():
            raise ObjectError('Cannot copy a replica at %s into a replica at %s', other._site_name(), self._site_name())

        # not doing anything, actually

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset_name())

        try:
            site = inventory.sites[self._site_name()]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site_name())

        replica = dataset.find_replica(site)
        updated = False

        if replica is None:
            replica = DatasetReplica(dataset, site)
    
            dataset.replicas.add(replica)
            site.add_dataset_replica(replica, add_block_replicas = False)

            updated = True
        elif check and (replica is self or replica == self):
            # identical object -> return False if check is requested
            pass
        else:
            replica.copy(self)
            updated = True

        if check:
            return replica, updated
        else:
            return replica

    def unlink_from(self, inventory):
        try:
            dataset = inventory.datasets[self._dataset_name()]
            site = inventory.sites[self._site_name()]
        except KeyError:
            return None

        replica = site.find_dataset_replica(dataset)
        if replica is None:
            return None

        replica.unlink()
        return replica

    def unlink(self):
        for site_partition in self._site.partitions.itervalues():
            try:
                site_partition.replicas.pop(self)
            except KeyError:
                pass

        self._site._dataset_replicas.pop(self._dataset)

        for block_replica in list(self.block_replicas):
            block_replica.unlink(dataset_replica = self, unlink_dataset_replica = False)

        self._dataset.replicas.remove(self)

    def write_into(self, store):
        store.save_datasetreplica(self)

    def delete_from(self, store):
        store.delete_datasetreplica(self)

    def is_last_copy(self):
        return len(self._dataset.replicas) == 1 and self._dataset.replicas[0] == self

    def is_complete(self):
        for block_replica in self.block_replicas:
            if not block_replica.is_complete:
                return False

        return True

    def is_partial(self):
        # has all block replicas -> not partial
        if len(self.block_replicas) == len(self._dataset.blocks):
            return False

        # all block replicas must be complete
        return self.is_complete()

    def is_full(self):
        # does not have all block replicas -> not full
        if len(self.block_replicas) != len(self._dataset.blocks):
            return False

        # all block replicas must be complete
        return self.is_complete()

    def last_block_created(self):
        # this is actually last *update* not create..
        if len(self.block_replicas) == 0:
            return 0
        else:
            return max(br.last_update for br in self.block_replicas)

    def size(self, physical = True):
        if physical:
            return sum(r.size for r in self.block_replicas)
        else:
            return sum(r.block.size for r in self.block_replicas)

    def find_block_replica(self, block, must_find = False):
        try:
            if type(block).__name__ == 'Block':
                return next(b for b in self.block_replicas if b.block == block)
            else:
                return next(b for b in self.block_replicas if b.block.name == block)

        except StopIteration:
            if must_find:
                raise ObjectError('Cannot find block replica %s/%s', self._site.name, block.full_name())
            else:
                return None

    def _dataset_name(self):
        if type(self._dataset) is str:
            return self._dataset
        else:
            return self._dataset.name

    def _site_name(self):
        if type(self._site) is str:
            return self._site
        else:
            return self._site.name
