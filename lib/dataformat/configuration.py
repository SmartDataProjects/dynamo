import os
import re

class Configuration(dict):
    """
    Configuration object. Basically a dict, but allows access to elements with getattr.
    Also translates string with pattern $(VARIABLE) to environment variable VARIABLE.
    """

    def __init__(self, **kwd):
        for key, value in kwd.items():
            if type(value) is str:
                matches = re.findall('\$\(([^\)]+)\)', value)
                for match in matches:
                    value = value.replace('$(%s)' % match, os.environ[match])
        
            if type(value) is dict:
                self[key] = Configuration(**value)
            else:
                self[key] = value

    def __getattr__(self, attr):
        return self[attr]
