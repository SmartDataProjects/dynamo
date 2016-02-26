from common.interface.phedex import PhEDExInterface
from common.interface.mysql import MySQLInterface

class Generator(object):
    """
    Generator of various objects with a storage for singleton objects.
    """

    _singletons = {}

    def __init__(self, cls):
        self._cls = cls

    def __call__(self):
        try:
            obj = Generator._singletons[self._cls]
        except KeyError:
            obj = self._cls()
            Generator._singletons[self._cls] = obj

        return obj
            

default_interface = {
    'status_probe': Generator(PhEDExInterface),
    'transfers': Generator(PhEDExInterface),
    'deletion': Generator(PhEDExInterface),
    'inventory': Generator(MySQLInterface)
}
