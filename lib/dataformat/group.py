from block import Block
from exceptions import ObjectError

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
        return self is other or (self._name == other._name and self._olevel is other._olevel)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        self._olevel = other._olevel

    def unlinked_clone(self):
        return Group(self._name, self._olevel)

    def embed_into(self, inventory, check = False):
        updated = False
        
        try:
            group = inventory.groups[self._name]
        except KeyError:
            group = self.unlinked_clone()
            inventory.groups.add(group)

            updated = True
        else:
            if check and (group is self or group == self):
                # identical object -> return False if check is requested
                pass
            else:
                group.copy(self)
                updated = True

        if check:
            return group, updated
        else:
            return group

    def delete_from(self, inventory):
        if self._name is None:
            raise ObjectError('Deletion of null group not allowed')

        # Pop the group from the main list. All block replicas owned by the group
        # will be disowned.
        # Update to block replicas will be propagated at by calling Group.delete_from
        # at each inventory instance.
        # Database update must be taken care of by persistency store delete_group().
        group = inventory.groups.pop(self._name)

        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if block_replica.group == group:
                        block_replica.group = inventory.groups[None]

        return [group]

    def write_into(self, store, delete = False):
        if self._name is None:
            return

        if delete:
            store.delete_group(self)
        else:
            store.save_group(self)

Group.null_group = Group(None)
