from dataformat.block import Block

class Group(object):
    """
    Represents a user group.
    olevel: ownership level: Dataset or Block
    """

    __slots__ = ['name', 'olevel']

    def __init__(self, name, olevel = Block):
        self.name = name
        self.olevel = olevel

    def __str__(self):
        return 'Group %s (olevel=%s)' % (self.name, self.olevel.__name__)

    def __repr__(self):
        return 'Group(\'%s\')' % (self.name)
    
    def copy(self, other):
        self.olevel = other.olevel

    def unlinked_clone(self):
        return Group(self.name, self.olevel)

    def linked_clone(self, inventory):
        group = Group(self.name, self.olevel)
        inventory.groups[group.name] = group

        return group
