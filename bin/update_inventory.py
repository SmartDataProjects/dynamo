#!/usr/bin/env python

import sys
from argparse import ArgumentParser

parser = ArgumentParser(description = 'Dynamo')
parser.add_argument('--config', '-c', metavar = 'CONFIG', dest = 'config', default = '', help = 'Configuration JSON.')
parser.add_argument('--updated-since', '-t', metavar = 'TIMESTAMP', dest = 'updated_since', default = 0, help = 'Unix timestamp of the last update.')
parser.add_argument('--dry-run', '-D', action = 'store_true', dest = 'dry_run', help = 'Do not make any actual deletion requests or changes to local databases.')

args = parser.parse_args()
sys.argv = []

from dataformat import Configuration
from common.misc import unicode2str
import source.impl

#dynamo: inventory

with open(args.config) as source:
    config_dict = json.loads(source.read())
    unicode2str(config_dict)
    config = Configuration(config_dict)

site_source = getattr(source.impl, config.sites.module)(config.sites.config)
group_source = getattr(source.impl, config.groups.module)(config.groups.config)
dataset_source = getattr(source.impl, config.datasets.module)(config.datasets.config)
replica_source = getattr(source.impl, config.replicas.module)(config.replicas.config)

## Fetch the full list of block replicas that were updated since updated_since.
## New groups, sites, datasets, and blocks will all follow.

for site in site_source.get_site_list():
    dynamo.update(site)

for replica in source_module.get_updated_replicas(args.updated_since):
    dynamo.update(replica)
