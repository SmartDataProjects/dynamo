class ApplicationLockInterface(object):
    """
    Interface to application locks.
    """

    def __init__(self, config):
        pass

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_value, traceback):
        self.unlock()
        return exc_type is None and exc_value is None and traceback is None

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')
