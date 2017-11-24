from dataformat.exceptions import ObjectError

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
        return 'DatasetReplica {site}:{dataset} (' \
            '{block_replicas_size} block_replicas)'.format(
                site = self._site.name, dataset = self._dataset.name,
                block_replicas_size = len(self.block_replicas))

    def __repr__(self):
        return 'DatasetReplica(%s, %s)' % (repr(self._dataset), repr(self._site))

    def __eq__(self, other):
        return self._dataset.name == other._dataset.name and self._site.name == other._site.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._dataset.name() != other._dataset.name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s', other._dataset.name, self._dataset.name)
        if self._site.name != other._site.name:
            raise ObjectError('Cannot copy a replica at %s into a replica at %s', other._site.name, self._site.name)

    def unlinked_clone(self):
        dataset = self._dataset.unlinked_clone()
        site = self._site.unlinked_clone()
        return DatasetReplica(dataset, site)

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._dataset.name]
        except KeyError:
            raise ObjectError('Unknown dataset %s', self._dataset.name)

        try:
            site = inventory.sites[self._site.name]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site.name)

        replica = dataset.find_replica(site)
        if replica is None:
            replica = DatasetReplica(dataset, site)
    
            dataset.replicas.add(replica)
            site.add_dataset_replica(replica)

            return True
        else:
            if replica is self:
                # identical object -> return False if check is requested
                return not check

            if check and replica == self:
                return False
            else:
                replica.copy(self)
                return True

    def delete_from(self, inventory):
        dataset = inventory.datasets[self._dataset.name]
        site = inventory.sites[self._site.name]
        replica = site.find_dataset_replica(dataset)

        dataset.replicas.remove(replica)
        for block_replica in replica.block_replicas:
            block_replica.block.replicas.remove(block_replica)

        site.remove_dataset_replica(replica)

    def write_into(self, store, delete = False):
        if delete:
            store.delete_datasetreplica(self)
        else:
            store.save_datasetreplica(self)

    def is_last_copy(self):
        return len(self._dataset.replicas) == 1 and self._dataset.replicas[0] == self

    def is_complete(self):
        for block_replica in self.block_replicas:
            if not block_replica.is_complete:
                return False

        return True

    def is_partial(self):
        # dataset.blocks must be loaded if a replica is created for the dataset

        # has all block replicas -> not partial
        if len(self.block_replicas) == len(self._dataset.blocks):
            return False

        # all block replicas must be complete
        return self.is_complete()

    def is_full(self):
        # dataset.blocks must be loaded if a replica is created for the dataset

        # does not have all block replicas -> not full
        if len(self.block_replicas) != len(self._dataset.blocks):
            return False

        # all block replicas must be complete
        return self.is_complete()

    def last_block_created(self):
        # this is actually last *update* not create..
        return max(br.last_update for br in self.block_replicas)

    def size(self, groups = [], physical = True):
        if type(groups) is not list:
            # single group given
            if physical:
                return sum([r.size for r in self.block_replicas if r.group == groups])
            else:
                return sum([r.block.size for r in self.block_replicas if r.group == groups])

        else: # expect a list
            if len(groups) == 0:
                # no group spec
                if self.is_full():
                    return self._dataset.size
                else:
                    if physical:
                        return sum([r.size for r in self.block_replicas])
                    else:
                        return sum([r.block.size for r in self.block_replicas])

            else:
                if physical:
                    return sum([r.size for r in self.block_replicas if r.group in groups])
                else:
                    return sum([r.block.size for r in self.block_replicas if r.group in groups])

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

    def remove_block_replica(self, block_replica):
        self.block_replicas.remove(block_replica)
        self._site.update_partitioning(self)
