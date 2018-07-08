from dynamo.utils.classutil import get_instance

class Applock(object):
    """
    Interface to application locks.
    """

    @staticmethod
    def get_instance(module, config):
        return get_instance(Applock, module, config)

    def __init__(self, config):
        pass

    def get_locked_apps(self):
        """
        @return  List of application names
        """
        raise NotImplementedError('get_locked_apps')
