#!/usr/bin/python

import collections
import json
import sys

import common.configuration as config
from common.inventory import InventoryManager
from common.interface.weblock import WebReplicaLock
from common.interface.mysqllock import MySQLReplicaLock
from common.interface.webservice import RESTService, POST

inventory = InventoryManager()

mysqlhost = 'https://t3serv012.mit.edu'

urls = [
    ('https://vocms049.cern.ch/unified/public/globallocks.json', 'noauth', 'LIST_OF_DATASETS', ('vlimant', 'unified')),
    ('https://cmst2.web.cern.ch/cmst2/unified-testbed/globallocks.json', 'cert', 'LIST_OF_DATASETS', ('vlimant', 'unified-testbed')),
    ('https://cmst1.web.cern.ch/CMST1/lockedData/lockTestSamples.json', 'cert', 'SITE_TO_DATASETS', ('vlimant', 'wmcore')),
    ('https://cmsweb.cern.ch/t0wmadatasvc/prod/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', ('dmytro', 't0-prod')),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replayone/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', ('dmytro', 't0-replayone')),
    ('https://cmsweb.cern.ch/t0wmadatasvc/replaytwo/dataset_locked', 'cert', 'CMSWEB_LIST_OF_DATASETS', ('dmytro', 't0-replaytwo'))
]

config.mysqllock.users = []

for url, auth_type, content_type, user in urls:
    weblock = WebReplicaLock(sources = [(url, auth_type, content_type)])

    weblock.update(inventory)

    data = []
    
    for dataset in inventory.datasets.values():
        try:
            locked_blocks = dataset.demand['locked_blocks']
        except KeyError:
            continue
    
        entries = []
    
        collapse_dataset = True

        locked_sites = set()
        for site, blocks in locked_blocks.items():
            replica = dataset.find_replica(site)
    
            blocks_in_replica = set()
            for block_replica in replica.block_replicas:
                blocks_in_replica.add(block_replica.block)

            if blocks_in_replica == blocks:
                entries.append({'item': dataset.name, 'sites': site.name, 'expires': '2017-12-31', 'comment': 'Auto-produced by dynamo'})
            else:
                for block in blocks:
                    entries.append({'item': dataset.name + '#' + block.real_name(), 'sites': site.name, 'expires': '2017-12-31', 'comment': 'Auto-produced by dynamo'})
    
                collapse_dataset = False

            if blocks != dataset.blocks:
                collapse_dataset = False

            locked_sites.add(site)

        if locked_sites != set(r.site for r in dataset.replicas):
            collapse_dataset = False
    
        if collapse_dataset:
            entries = [{'item': dataset.name, 'expires': '2017-12-31', 'comment': 'Auto-produced by dynamo'}]
    
        data.extend(entries)
    
        dataset.demand.pop('locked_blocks')

    service = RESTService(mysqlhost + '/registry/detoxlock')
    service.make_request('set?asuser=%s&service=%s' % user, method = POST, options = data, format = 'json')
