class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self):
        self.debug_mode = False

    def schedule_deletion(self, replica):
        """
        Schedule a deletion of the dataset or block replica.
        """

        pass
