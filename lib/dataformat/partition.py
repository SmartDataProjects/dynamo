from sitepartition import SitePartition

class Partition(object):
    """
    Defines storage partitioning. Instances must replace contains() to a callable
    that returns True when the passed block replica belongs to the partition.
    """

    __slots__ = ['_name', '_subpartitions', '_parent', '_condition']

    @property
    def name(self):
        return self._name

    @property
    def subpartitions(self):
        return self._subpartitions

    @property
    def parent(self):
        return self._parent

    def __init__(self, name, condition = None):
        self._name = name
        self._subpartitions = None
        self._parent = None

        # Members that cannot be exported in a pickle (and thus cannot be communicated
        # through multiprocessing queues) - excluded in __getstate__
        self._condition = condition

    def __str__(self):
        return 'Partition %s' % self._name

    def __repr__(self):
        return 'Partition(name=\'%s\')' % self._name

    def __eq__(self, other):
        # only comparing names since the rest are set by configuration and are basically constants
        return self._name == other._name

    def __ne__(self, other):
        return self._name != other._name

    def __getstate__(self):
        return {'_name': self._name, '_subpartitions': self._subpartitions, '_parent': self._parent, '_condition': None}

    def __setstate__(self, state):
        # Need this function because Partition does not have __dict__
        for key, value in state.iteritems():
            setattr(self, key, value)

    def copy(self, other):
        pass

    def unlinked_clone(self):
        return Partition(self._name)

    def embed_into(self, inventory, check = False):
        updated = False

        try:
            partition = inventory.partitions[self._name]
        except KeyError:
            partition = self.unlinked_clone()
    
            if self._subpartitions is not None:
                partition._subpartitions = []
                for subp in self._subpartitions:
                    partition._subpartitions.append(inventory.partitions[subp._name])
    
            if self._parent is not None:
                partition._parent = inventory.partitions[self._parent._name]
    
            inventory.partitions.add(partition)

            # update the site partition list at sites
            for site in inventory.sites.itervalues():
                site.partitions[partition] = SitePartition(site, partition)

            updated = True
        else:
            if check and (partition is self or partition == self):
                # identical object -> return False if check is requested
                pass
            else:
                partition.copy(self)
                updated = True

        if check:
            return partition, updated
        else:
            return partition

    def delete_from(self, inventory):
        # Pop the partition from the main list, and remove site_partitions.
        partition = inventory.partitions.pop(self._name)

        for site in inventory.sites.itervalues():
            site.partitions.pop(partition)

    def write_into(self, store, delete = False):
        if delete:
            store.delete_partition(self)
        else:
            store.save_partition(self)

    def contains(self, replica):
        if self._subpartitions is None:
            return self._condition.match(replica)
        else:
            for subp in self._subpartitions:
                if subp.contains(replica):
                    return True

            return False

    def embed_tree(self, inventory):
        partition = self.unlinked_clone()
        inventory.partitions.add(partition)

        # update the site partition list at sites
        for site in inventory.sites.itervalues():
            site.partitions[partition] = SitePartition(site, partition)

        if self._subpartitions is not None:
            partition._subpartitions = []

            for subp in self._subpartitions:
                new_subp = subp.embed_tree(inventory)
                partition._subpartitions.append(new_subp)
                new_subp._parent = partition

        return partition
