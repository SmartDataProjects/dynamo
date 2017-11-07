import copy

class Partition(object):
    """
    Defines storage partitioning. Instances must replace contains() to a callable
    that returns True when the passed block replica belongs to the partition.
    """

    __slots__ = ['name', 'subpartitions', 'parent', '_condition']

    def __init__(self, name, condition = None):
        self.name = name
        self.subpartitions = None
        self.parent = None
        self._condition = condition

    def __str__(self):
        return 'Partition %s' % self.name

    def __repr__(self):
        return 'Partition(name=\'%s\')' % self.name

    def copy(self, other):
        self._condition = copy.deepcopy(self._condition)

    def unlinked_clone(self):
        return Partition(self.name, copy.deepcopy(self._condition))

    def linked_clone(self, inventory):
        partition = self.unlinked_clone()

        if self.subpartitions is not None:
            partition.subpartitions = []
            for subp in self.subpartitions:
                partition.subpartions.append(inventory.partitions[subp.name])

        if self.parent is not None:
            partition.parent = inventory.partitions[self.parent.name]

        inventory.partitions[partition.name] = partition

        return partition

    def contains(self, replica):
        if self.subpartitions is None:
            return self._condition.match(replica)
        else:
            for subp in self.subpartitions:
                if subp.contains(replica):
                    return True

            return False

