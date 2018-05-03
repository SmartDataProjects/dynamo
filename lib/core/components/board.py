class UpdateBoard(object):
    """
    Interface to local and remote "board" to register asynchronous inventory updates.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, UpdateBoard):
            raise RuntimeError('%s is not a subclass of UpdateBoard' % module)

        return cls(config)

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
