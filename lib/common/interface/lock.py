class ReplicaLockInterface(object):
    """
    Interface to replica locks.
    """

    def __init__(self):
        self.locked_blocks = {} # {dataset: [blockreplica]}

    def update(self, inventory):
        """
        Read the source and fill in self.locked_blocks array with block replicas that are locked.
        """
        pass
