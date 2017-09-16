class Generator(object):
    """
    Generator of various objects with a storage for singleton objects.
    """

    _singletons = {}

    def __init__(self, modname, clsname):
        self._modname = modname
        self._clsname = clsname

    def __call__(self):
        try:
            obj = Generator._singletons[self._clsname]
        except KeyError:
            imp = __import__('common.interface.' + self._modname, globals(), locals(), [self._clsname], -1)
            obj = getattr(imp, self._clsname)()
            Generator._singletons[self._clsname] = obj

        return obj


default_interface = {
    'dataset_source': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'site_source': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'user_source': Generator('sitedb', 'SiteDB'),
    'group_source': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'replica_source': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'copy': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'deletion': Generator('phedexdbsssb', 'PhEDExDBSSSB'),
    'store': Generator('mysqlstore', 'MySQLStore'),
    'history': Generator('mysqlhistory', 'MySQLHistory')
}

demand_plugins = {
    'replica_locks': Generator('mysqllock', 'MySQLReplicaLock'),
    'replica_access': Generator('popdb', 'PopDB'),
    'replica_demands': Generator('localaccess', 'LocalAccess'),
    'dataset_request': Generator('globalqueue', 'GlobalQueue')
}
