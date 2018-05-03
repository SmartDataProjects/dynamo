class ApplicationLockInterface(object):
    """
    Interface to application locks.
    """

    def __init__(self, config, authorizer):
        self.user = config.user
        self.role = config.role
        self.app = config.app

        if not authorizer.check_user_auth(self.user, self.role, 'registry'):
            raise RuntimeError('User not authorized to use application lock.')

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_value, traceback):
        self.unlock()
        return exc_type is None and exc_value is None and traceback is None

    def check(self):
        """
        Return (user, role) that owns the current lock. If unlocked, return None.
        """
        raise NotImplementedError('check')

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')
