import os
import json

from dataformat import Configuration

if 'DYNAMO_CONFIG' in os.environ:
    config_path = os.environ['DYNAMO_CONFIG']
else:
    config_path = '/etc/dynamo/config.json'

with open(config_path) as source:
    common_config = Configuration(source)
