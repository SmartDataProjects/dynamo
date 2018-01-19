import os
import re
import json

from dynamo.utils.transform import unicode2str

class Configuration(dict):
    """
    Configuration object. Basically a dict, but allows access to elements with getattr.
    Also translates string with pattern $(VARIABLE) to environment variable VARIABLE.
    """

    def __init__(self, config = dict()):
        if type(config) is file:
            config = json.loads(config.read())
            unicode2str(config)
        elif type(config) is str:
            with open(config) as source:
                config = json.loads(source.read())
                unicode2str(config)

        for key, value in config.iteritems():
            if type(value) is str:
                matches = re.findall('\$\(([^\)]+)\)', value)
                for match in matches:
                    value = value.replace('$(%s)' % match, os.environ[match])
        
            if type(value) is dict or type(value) is Configuration:
                self[key] = Configuration(value)
            else:
                self[key] = value

    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value

    def get(self, attr, default):
        """Return the default value if attr is not found."""
        try:
            return self[attr]
        except KeyError:
            return default

    def clone(self):
        return Configuration(self)
