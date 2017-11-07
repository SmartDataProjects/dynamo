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

    def contains(self, replica):
        if self.subpartitions is None:
            return self._condition.match(replica)
        else:
            for subp in self.subpartitions:
                if subp.contains(replica):
                    return True

            return False

