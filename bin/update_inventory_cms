#!/usr/bin/env python

import sys
from argparse import ArgumentParser

parser = ArgumentParser(description = 'Dynamo')
parser.add_argument('--config', '-c', metavar = 'CONFIG', dest = 'config', default = '', help = 'Configuration JSON.')
parser.add_argument('--updated-since', '-t', metavar = 'TIMESTAMP', dest = 'updated_since', default = 0, help = 'Unix timestamp of the last update.')
parser.add_argument('--dry-run', '-D', action = 'store_true', dest = 'dry_run', help = 'Do not make any actual deletion requests or changes to local databases.')

args = parser.parse_args()
sys.argv = []

from dataformat import Configuration, DatasetReplica
import source.impl

#dynamo: inventory

with open(args.config) as source:
    config = Configuration(source)

group_source = getattr(source.impl, config.groups.module)(config.groups.config)
site_source = getattr(source.impl, config.sites.module)(config.sites.config)
dataset_source = getattr(source.impl, config.datasets.module)(config.datasets.config)
replica_source = getattr(source.impl, config.replicas.module)(config.replicas.config)

for group in group_source.get_group_list():
    dynamo.update(group, check = True)

for site in site_source.get_site_list():
    dynamo.update(site, check = True)

## Fetch the full list of block replicas that were updated since updated_since.
## New datasets and blocks will be caught in the process.

for replica in replica_source.get_updated_replicas(args.updated_since):
    # pick up replicas only at known groups and sites
    if replica.group is not None and replica.group.name not in dynamo.groups:
        continue

    try:
        site = dynamo.sites[replica.site.name]
    except KeyError:
        continue

    try:
        dataset = dynamo.datasets[replica.block.dataset.name]
    except KeyError:
        dataset = dataset_source.get_dataset(replica.block.dataset.name)

    dynamo.update(dataset, check = True)
    for block in dataset.blocks:
        dynamo.update(block, check = True)

    block = dataset.find_block(replica.block.name)
    if block is None:
        block = dataset_source.get_block(replica.block.full_name())

    dynamo.update(block, check = True)

    dataset_replica = dataset.find_replica(site)
    if dataset_replica is None:
        dataset_replica = DatasetReplica(dataset, site)

    dynamo.update(dataset_replica, check = True)

    if replica.is_custodial and not dataset_replica.is_custodial:
        dataset_replica.is_custodial = True
        dynamo.update(dataset_replica)
    
    dynamo.update(replica)

## Repeat for deleted block replicas.

for replica in replica_source.get_deleted_replicas(args.updated_since):
    # blockreplica.delete_from() raises a KeyError or ObjectError if
    # any of the group, site, dataset, ... is not found
    try:
        dynamo.delete(replica)
    except KeyError:
        pass
    except ObjectError:
        pass
