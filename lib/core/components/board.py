from dynamo.utils.classutil import get_instance

class UpdateBoard(object):
    """
    Interface to local and remote "board" to register asynchronous inventory updates.
    """

    @staticmethod
    def get_instance(module, config):
        return get_instance(UpdateBoard, module, config)

    def __init__(self, config):
        pass

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')

    def get_updates(self):
        raise NotImplementedError('get_updates')

    def flush(self):
        raise NotImplementedError('flush')

    def write_updates(self, update_commands):
        raise NotImplementedError('write_updates')
