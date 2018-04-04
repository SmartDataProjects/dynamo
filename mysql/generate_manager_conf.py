import os
import sys
import json

thisdir = os.path.dirname(os.path.realpath(__file__))

def generate_manager_conf(conf_str):
    conf = json.loads(conf_str)

    with open(thisdir + '/grants.json') as source:
        grants_conf = json.load(source)

    if 'master_host' not in conf:
        host = 'localhost'
    else:
        host = conf['master_host']

    user = conf['user']
    if 'passwd' not in conf:
        passwd = grants_conf[user]['passwd']
        
    conf_str = '''
    "module": "MySQLServerManager",
    "config": {
      "master_server": {
        "host": "''' + host + '''",
        "user": "''' + user + '''",
        "passwd": "''' + passwd + '''",
        "db": "dynamoserver"
      }
    }
'''

    return conf_str

try:
    __namespace__.generate_manager_conf = generate_manager_conf
except NameError:
    pass
