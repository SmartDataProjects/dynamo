import os
import sys
import json

thisdir = os.path.dirname(os.path.realpath(__file__))

def generate_master_conf(conf_str):
    conf = json.loads(conf_str)

    with open(thisdir + '/grants.json') as source:
        grants_conf = json.load(source)

    if 'host' not in conf:
        host = 'localhost'
    else:
        host = conf['host']

    user = conf['user']
    if 'passwd' in conf:
        passwd = conf['passwd']
    else:
        passwd = grants_conf[user]['passwd']
        
    conf_str = '''
      "module": "MySQLMasterServer",
      "config": {
        "db_params": {
          "host": "''' + host '''",
          "user": "''' + user + '''",
          "passwd": "''' + passwd + '''",
          "db": "dynamoserver"
        }
      }'''

    return conf_str

try:
    __namespace__.generate_master_conf = generate_master_conf
except NameError:
    pass
