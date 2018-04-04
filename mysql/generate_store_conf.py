import os
import sys
import json

thisdir = os.path.dirname(os.path.realpath(__file__))

def generate_store_conf(conf_str):
    conf = json.loads(conf_str)

    with open(thisdir + '/grants.json') as source:
        grants_conf = json.load(source)
    
    server_conf = grants_conf[conf['server']]
    reader_conf = grants_conf[conf['reader']]
    
    conf_str = '''
      "module": "MySQLInventoryStore",
      "config": {
        "db_params": {
          "host": "localhost",
          "db": "dynamo",
          "reuse_connection": true,
          "user": "''' + conf['server'] + '''",
          "passwd": "''' + server_conf['passwd'] + '''"
        }
      },
      "readonly_config": {
        "db_params": {
          "host": "localhost",
          "db": "dynamo",
          "reuse_connection": true,
          "user": "''' + conf['reader'] + '''",
          "passwd": "''' + reader_conf['passwd'] + '''"
        }
      }
'''

    return conf_str

try:
    __namespace__.generate_store_conf = generate_store_conf
except NameError:
    pass
