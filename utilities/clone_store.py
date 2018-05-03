#!/usr/bin/env python

#######################################################################
## Clone the content of a remote inventory store.
## This version only handles MySQLInventoryStore and assumes MySQL
## user and password are identical to the local store. Need to add
## command line options etc. for more general cases.
#######################################################################

import os
import sys
import logging
from argparse import ArgumentParser

parser = ArgumentParser(description = 'Parse configuration files')
parser.add_argument('source', metavar = 'HOST', help = 'Source host to copy the inventory from.')

args = parser.parse_args()
sys.argv = []

try:
    debug = (os.environ['DYNAMO_SERVER_DEBUG'] == '1')
except:
    debug = False

if not debug:
    if os.geteuid() != 0:
        sys.stderr.write('Root privilege required\n')
        sys.exit(1)

logging.basicConfig(level = logging.INFO)
LOG = logging.getLogger()

from dynamo.dataformat import Configuration

try:
    config_path = os.environ['DYNAMO_SERVER_CONFIG']
except KeyError:
    config_path = '/etc/dynamo/server_config.json'

config = Configuration(config_path)

from dynamo.core.components.persistency import InventoryStore

local_config = config.inventory.persistency.config
remote_config = config.inventory.persistency.readonly_config
# Here is where MySQLInventoryStore + same user/password is assumed
remote_config.db_params.host = args.source

local = InventoryStore.get_instance(config.inventory.persistency.module, local_config)
# Another place where common storage technology is assumed
remote = InventoryStore.get_instance(config.inventory.persistency.module, remote_config)

LOG.info('Cloning inventory store from %s', args.source)

local.clone_from(remote)

LOG.info('Done.')
