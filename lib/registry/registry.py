import os

class DynamoRegistry(object):
    """
    An interface to the world with a web frontend and a DB backend.
    Due to its installation-specific nature, the backend methods of the registry are defined in the
    subdirectory dynamo.registry.methods which are dynamically bound at the bottom of this file.
    """

    _methods_loaded = False

    def __init__(self, config):
        # Backend DB object dynamically loaded (including the module and class names from config)
        modname, clsname = config.backend.module.split(':')
        module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
        cls = getattr(module, clsname)

        self.backend = cls(config.backend.config)

        # ugly as hell.. this block loads all registry methods at the first instantiation
        if not DynamoRegistry._methods_loaded:
            DynamoRegistry._methods_loaded = True

            modbase = __name__[:__name__.rfind('.')] + '.methods.'
            for pyfile in os.listdir(os.path.dirname(__file__) + '/methods'):
                if pyfile.startswith('_') or not pyfile.endswith('.py'):
                    continue

                __import__(modbase + pyfile[:-3], globals(), locals())
