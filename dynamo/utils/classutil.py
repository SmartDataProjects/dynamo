def get_instance(base, module, *args):
    modname, _, clsname = module.partition(':') # e.g. mysqlstore:MySQLInventoryStore
    pkg = base.__module__ # e.g. dynamo.core.components.persistency
    # equivalent to "from dynamo.core.components.impl.mysqlstore import MySQLInventoryStore"
    cls = getattr(__import__(pkg[:pkg.rfind('.')] + '.impl.' + modname, globals(), locals(), [clsname]), clsname)

    if not issubclass(cls, base):
        raise RuntimeError('%s is not a subclass of %s' % (clsname, base.__name__))

    return cls(*args)
