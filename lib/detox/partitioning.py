"""
Classes defining replica requirements.
"""

class BelongsTo(object):
    def __init__(self, group):
        self.group = group

    def dataset(self, replica):
        if replica.group is None:
            return -1
        elif replica.group == self.group:
            return 1
        else:
            return 0

    def block(self, replica):
        return replica.group == self.group
