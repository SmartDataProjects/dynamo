import os
import re
import json

from dynamo.utils.transform import unicode2str

class Configuration(dict):
    """
    Configuration object. Basically a dict, but allows access to elements with getattr.
    Also translates string with pattern $(VARIABLE) to environment variable VARIABLE.
    """

    def __init__(self, _arg = None, **kwd):
        if _arg is None:
            config = dict()
        elif type(_arg) is dict or type(_arg) is Configuration:
            config = dict(_arg)
        elif type(_arg) is file:
            config = json.loads(_arg.read())
            unicode2str(config)
        elif type(_arg) is str:
            with open(_arg) as source:
                config = json.loads(source.read())
                unicode2str(config)

        config.update(kwd)

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

    def __repr__(self):
        return self.dump_json()

    def get(self, attr, default):
        """Return the default value if attr is not found."""
        try:
            return self[attr]
        except KeyError:
            return default

    def clone(self):
        return Configuration(self)

    def dump_json(self):
        json = '{'
        for key, value in self.iteritems():
            if len(json) != 1:
                json += ','

            json += '"%s":%s' % (key, repr(value))

        json += '}'

        return json
