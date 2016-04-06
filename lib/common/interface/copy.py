class CopyInterface(object):
    """
    Interface to data copy application.
    """

    def __init__(self):
        pass

    def schedule_copy(self, dataset, dest, origin = None, comments = ''):
        """
        Schedule and execute a copy operation. Argument origin can be None for copy interfaces
        that do not require the origin to be specified.
        """

        pass
