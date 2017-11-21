from dataformat.block import Block

class Group(object):
    """
    Represents a user group.
    olevel: ownership level: Dataset or Block
    """

    __slots__ = ['_name', '_olevel']

    @property
    def name(self):
        return self._name

    @property
    def olevel(self):
        return self._olevel

    def __init__(self, name, olevel = Block):
        self._name = name
        self._olevel = olevel

    def __str__(self):
        return 'Group %s (olevel=%s)' % (self._name, self._olevel.__name__)

    def __repr__(self):
        return 'Group(\'%s\')' % (self._name)

    def __eq__(self, other):
        # will only compare names (olevel is set by configuration and is basically constant)
        return self._name == other._name

    def __ne__(self, other):
        return self._name != other._name
    
    def copy(self, other):
        pass

    def unlinked_clone(self):
        return Group(self._name, self._olevel)

    def embed_into(self, inventory, check = False):
        try:
            group = inventory.groups[self._name]
        except KeyError:
            group = self.unlinked_clone()
            inventory.groups.add(group)

            return True
        else:
            if group is self:
                # identical object -> return False if check is requested
                return not check

            if check and group == self:
                return False
            else:
                group.copy(self)
                return True

    def delete_from(self, inventory):
        # Pop the group from the main list. All block replicas owned by the group
        # will be disowned.
        group = inventory.groups.pop(self.name)

        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if block_replica.group == group:
                        block_replica.group = None

    def write_into(self, store, delete = False):
        if delete:
            store.delete_group(self)
        else:
            store.save_group(self)
