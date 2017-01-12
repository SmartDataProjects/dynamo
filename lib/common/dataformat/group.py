from block import Block

class Group(object):
    """
    Represents a user group.
    olevel: ownership level: Dataset or Block
    """

    def __init__(self, name, olevel = Block):
        self.name = name
        self.olevel = olevel

    def __str__(self):
        return 'Group %s (olevel=%s)' % (self.name, self.olevel.__name__)

    def __repr__(self):
        return 'Group(\'%s\', %s)' % (self.name, self.olevel.__name__)

