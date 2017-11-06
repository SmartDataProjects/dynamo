import os
import json

from dataformat import Configuration
from common.misc import unicode2str

if 'DYNAMO_CONFIG' in os.environ:
    config_path = os.environ['DYNAMO_CONFIG']
else:
    config_path = '/etc/dynamo/config.json'

with open(config_path) as source:
    config_dict = json.loads(source.read())
    unicode2str(config_dict)
    common_config = Configuration(**config_dict)
