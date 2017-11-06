import os
import re
import json

class Configuration(dict):
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

if 'DYNAMO_CONFIG' in os.environ:
    config_path = os.environ['DYNAMO_CONFIG']
else:
    config_path = '/etc/dynamo/config.json'

with open(config_path) as source:
    config_dict = json.loads(source.read())
    common_config = Configuration(**config_dict)
