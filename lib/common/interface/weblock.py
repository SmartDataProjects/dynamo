import logging
import collections
import urllib2
import time

import common.interface.webservice as webservice
from common.dataformat import Block
import common.configuration as config

logger = logging.getLogger(__name__)

class WebReplicaLockInterface(object):
    """
    A plugin for DemandManager that appends lists of blocks that are locked.
    Sets one demand value:
      locked_blocks:   {site: set of blocks}
    """

    # content types
    LIST_OF_DATASETS, CMSWEB_LIST_OF_DATASETS, SITE_TO_DATASETS = range(3)

    def __init__(self, sources = config.weblock.sources):
        self._sources = [] # [(RESTService, content type)]

        for source in sources:
            self.add_source(*source)

    def add_source(self, url, auth_type, content_type, data_type = 'application/json'):
        if type(content_type) is str:
            content_type = eval('WebReplicaLockInterface.' + content_type)

        if auth_type == 'cert':
            auth_handler = webservice.HTTPSCertKeyHandler
        elif auth_type == 'cookie':
            auth_handler = webservice.CERNSSOCookieAuthHandler
        elif auth_type == 'noauth':
            auth_handler = None

        self._sources.append((webservice.RESTService(url, accept = data_type, auth_handler = auth_handler), content_type))

    def load(self, inventory):
        self.update(inventory)

    def update(self, inventory):
        # check that the lock files themselves are not locked
        while True:
            try:
                urllib2.urlopen(config.weblock.lock)
            except urllib2.HTTPError as err:
                if err.code == 404:
                    # file not found -> no lock
                    break
                else:
                    raise

            logger.info('Lock files are being produced. Waiting 60 seconds.')
            time.sleep(60)

        for source, content_type in self._sources:
            logger.info('Retrieving lock information from %s', source.url_base)

            data = source.make_request()

            if content_type == WebReplicaLockInterface.LIST_OF_DATASETS:
                # simple list of datasets
                for dataset_name in data:
                    if dataset_name is None:
                        logger.debug('Dataset name None found in %s', source.url_base)
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        logger.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                        continue

                    if dataset.replicas is None:
                        continue

                    try:
                        locked_blocks = dataset.demand['locked_blocks']
                    except KeyError:
                        locked_blocks = dataset.demand['locked_blocks'] = {}

                    for replica in dataset.replicas:
                        if replica.site in locked_blocks:
                            locked_blocks[replica.site].update(brep.block for brep in replica.block_replicas)
                        else:
                            locked_blocks[replica.site] = set(brep.block for brep in replica.block_replicas)

            elif content_type == WebReplicaLockInterface.CMSWEB_LIST_OF_DATASETS:
                # data['result'] -> simple list of datasets
                for dataset_name in data['result']:
                    if dataset_name is None:
                        logger.debug('Dataset name None found in %s', source.url_base)
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        logger.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                        continue

                    if dataset.replicas is None:
                        continue

                    try:
                        locked_blocks = dataset.demand['locked_blocks']
                    except KeyError:
                        locked_blocks = dataset.demand['locked_blocks'] = {}

                    for replica in dataset.replicas:
                        if replica.site in locked_blocks:
                            locked_blocks[replica.site].update(brep.block for brep in replica.block_replicas)
                        else:
                            locked_blocks[replica.site] = set(brep.block for brep in replica.block_replicas)
                
            elif content_type == WebReplicaLockInterface.SITE_TO_DATASETS:
                # data = {site: {dataset: info}}
                for site_name, objects in data.items():
                    try:
                        site = inventory.sites[site_name]
                    except KeyError:
                        logger.debug('Unknown site %s in %s', site_name, source.url_base)
                        continue

                    for object_name, info in objects.items():
                        if not info['lock']:
                            logger.debug('Object %s is not locked at %s', object_name, site_name)
                            continue

                        if '#' in object_name:
                            dataset_name, block_real_name = object_name.split('#')
                        else:
                            dataset_name = object_name
                            block_real_name = None

                        try:
                            dataset = inventory.datasets[dataset_name]
                        except KeyError:
                            logger.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                            continue

                        if dataset.replicas is None:
                            continue

                        replica = dataset.find_replica(site)
                        if replica is None:
                            logger.debug('Replica of %s is not at %s in %s', dataset_name, site_name, source.url_base)
                            continue

                        if dataset.blocks is None:
                            inventory.store.load_blocks(dataset)

                        if block_real_name is None:
                            blocks = list(dataset.blocks)
                        else:
                            block = dataset.find_block(Block.translate_name(block_real_name))
                            if block is None:
                                logger.debug('Unknown block %s of %s in %s', block_real_name, dataset_name, source.url_base)
                                continue

                            blocks = [block]

                        try:
                            locked_blocks = dataset.demand['locked_blocks']
                        except KeyError:
                            locked_blocks = dataset.demand['locked_blocks'] = {}
    
                        if site in locked_blocks:
                            locked_blocks[site].update(blocks)
                        else:
                            locked_blocks[site] = set(blocks)


if __name__ == '__main__':
    # Unit test

    import pprint
    from common.inventory import InventoryManager

    logger.setLevel(logging.DEBUG)

    inventory = InventoryManager()
    locks = WebReplicaLockInterface()

    locks.update(inventory)

    all_locks = []

    for dataset in inventory.datasets.values():
        try:
            locked_blocks = dataset.demand['locked_blocks']
        except KeyError:
            continue

        for site, blocks in locked_blocks.items():
            if blocks == set(dataset.blocks):
                all_locks.append((site.name, dataset.name))
            else:
                all_locks.append((site.name, dataset.name, [b.real_name() for b in blocks]))

    pprint.pprint(all_locks)
