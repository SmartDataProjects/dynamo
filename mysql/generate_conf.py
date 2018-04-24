import os
import sys
from collections import OrderedDict as OD
import json

thisdir = os.path.dirname(os.path.realpath(__file__))

def generate_local_board_conf(conf_str):
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

    board_conf = OD({
        'module': 'MySQLUpdateBoard',
        'config': OD()
    })
    
    board_conf['config']['db_params'] = OD({
        'host': host,
        'user': user,
        'passwd': passwd,
        'db': 'dynamoserver'
    })

    return board_conf

def generate_store_conf(conf_str):
    conf = json.loads(conf_str)

    with open(thisdir + '/grants.json') as source:
        grants_conf = json.load(source)
    
    server_conf = grants_conf[conf['server']]
    reader_conf = grants_conf[conf['reader']]

    store_conf = OD({
        'module': 'MySQLInventoryStore',
        'config': OD(),
        'readonly_config': OD()
    })

    store_conf['config']['db_params'] = OD({
        'host': 'localhost',
        'db': 'dynamo',
        'reuse_connection': True,
        'user': conf['server'],
        'passwd': server_conf['passwd']
    })

    store_conf['readonly_config']['db_params'] = OD({
        'host': 'localhost',
        'db': 'dynamo',
        'reuse_connection': True,
        'user': conf['reader'],
        'passwd': reader_conf['passwd']
    })

    return store_conf

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

    master_conf = OD({
        'module': 'MySQLMasterServer',
        'config': OD()
    })

    master_conf['config']['db_params'] = OD({
        'host': host,
        'user': user,
        'passwd': passwd,
        'db': 'dynamoserver'
    })

    return master_conf

def generate_registry_conf(conf_str):
    conf = json.loads(conf_str)

    with open(thisdir + '/grants.json') as source:
        grants_conf = json.load(source)

    writer_conf = grants_conf[conf['writer']]
    reader_conf = grants_conf[conf['reader']]

    registry_conf = OD({
        'module': 'MySQLRegistry',
        'read_config': OD(),
        'write_config': OD()
    })

    if 'host' not in writer_conf:
        host = 'localhost'
    else:
        host = writer_conf['host']

    registry_conf['write_config']['db_params'] = OD({
        'host': host,
        'user': conf['writer'],
        'passwd': writer_conf['passwd'],
        'db': 'dynamoregister',
        'reuse_connection': True
    })

    if 'host' not in reader_conf:
        host = 'localhost'
    else:
        host = reader_conf['host']

    registry_conf['read_config']['db_params'] = OD({
        'host': host,
        'user': conf['reader'],
        'passwd': reader_conf['passwd'],
        'db': 'dynamoregister',
        'reuse_connection': True
    })

    return registry_conf

try:
    __namespace__.generate_store_conf = generate_store_conf
    __namespace__.generate_master_conf = generate_master_conf
    __namespace__.generate_local_board_conf = generate_local_board_conf
    __namespace__.generate_registry_conf = generate_registry_conf
except NameError:
    pass
