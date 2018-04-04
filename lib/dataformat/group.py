from exceptions import ObjectError

class Group(object):
    """
    Represents a user group.
    olevel: ownership level: Dataset or Block
    """

    __slots__ = ['_name', 'id', '_olevel']

    _ownership_levels = ['Dataset', 'Block']
    OL_DATASET, OL_BLOCK = range(1, len(_ownership_levels) + 1)

    @property
    def name(self):
        return self._name

    @property
    def olevel(self):
        return self._olevel

    @staticmethod
    def olevel_val(arg):
        if type(arg) is str:
            return eval('Group.OL_' + arg.upper())
        else:
            return arg

    @staticmethod
    def olevel_name(arg):
        if type(arg) is int:
            return Group._ownership_levels[arg - 1]
        else:
            return arg

    def __init__(self, name, olevel = OL_BLOCK, gid = 0):
        self._name = name
        self._olevel = Group.olevel_val(olevel)

        self.id = gid

    def __str__(self):
        return 'Group %s (olevel=%s, id=%d)' % (self._name, Group.olevel_name(self._olevel), self.id)

    def __repr__(self):
        return 'Group(\'%s\',\'%s\',%d)' % (self._name,Group.olevel_name(self._olevel), self.id)

    def __eq__(self, other):
        return self is other or (self._name == other._name and self._olevel == other._olevel)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        self.id = other.id
        self._olevel = other._olevel

    def embed_into(self, inventory, check = False):
        updated = False
        
        try:
            group = inventory.groups[self._name]
        except KeyError:
            group = Group(self._name, self._olevel, self.id)
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

    def unlink_from(self, inventory):
        if self._name is None:
            raise ObjectError('Deletion of null group not allowed')

        # Pop the group from the main list. All block replicas owned by the group
        # will be disowned.
        # Update to block replicas will be propagated at by calling Group.unlink_from
        # at each inventory instance.
        # Database update must be taken care of by persistency store delete_group().
        try:
            group = inventory.groups.pop(self._name)
        except KeyError:
            return None

        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if block_replica.group == group:
                        block_replica.group = inventory.groups[None]

        return group

    def write_into(self, store):
        if self._name is None:
            return

        store.save_group(self)

    def delete_from(self, store):
        if self._name is None:
            raise ObjectError('Deletion of null group not allowed')

        store.delete_group(self)

Group.null_group = Group(None)
