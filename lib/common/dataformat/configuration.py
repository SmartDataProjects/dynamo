"""
Just a convenience class used to group configuration options.
"""

import pprint

class Configuration(object):
    def __init__(self, **kwd):
        for key, value in kwd.items():
            setattr(self, key, value)

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def isset(self, name):
        return hasattr(self, name)
