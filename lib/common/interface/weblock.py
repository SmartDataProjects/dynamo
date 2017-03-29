import logging
import collections
import urllib2
import time

from common.interface.lock import ReplicaLockInterface
import common.interface.webservice as webservice
from common.dataformat import Block
import common.configuration as config

logger = logging.getLogger(__name__)

class WebReplicaLockInterface(ReplicaLockInterface):
    """
    Implementation of ReplicaLockInterface using JSON/XML files read from given URLs.
    """

    # content types
    LIST_OF_DATASETS, CMSWEB_LIST_OF_DATASETS, SITE_TO_DATASETS = range(3)

    def __init__(self, sources = config.weblock.sources):
        ReplicaLockInterface.__init__(self)

        self._sources = [] # [(RESTService, content type)]

        for source in sources:
            self.add_source(*source)

        self.locked_blocks = collections.defaultdict(list)
        
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

    def update(self, inventory): #override

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

        self.locked_blocks = collections.defaultdict(list)

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

                    for dataset_replica in dataset.replicas:
                        for block_replica in dataset_replica.block_replicas:
                            self.locked_blocks[dataset].append(block_replica)

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

                    for dataset_replica in dataset.replicas:
                        for block_replica in dataset_replica.block_replicas:
                            self.locked_blocks[dataset].append(block_replica)
                
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
                            block_name = Block.translate_name(block_real_name)
                            try:
                                dataset = inventory.datasets[dataset_name]
                            except KeyError:
                                logger.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                                continue

                            block = dataset.find_block(block_name)
                            if block is None:
                                logger.debug('Unknown block %s of %s in %s', block_real_name, dataset_name, source.url_base)
                                continue

                            locked_blocks = [block]

                        else:
                            dataset_name = object_name
                            try:
                                dataset = inventory.datasets[dataset_name]
                            except KeyError:
                                logger.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                                continue

                            locked_blocks = list(dataset.blocks)

                        replica = dataset.find_replica(site)
                        if replica is None:
                            logger.debug('Replica of %s is not at %s in %s', dataset_name, site_name, source.url_base)
                            continue

                        for block_replica in replica.block_replicas:
                            if block_replica.block in locked_blocks:
                                self.locked_blocks[dataset].append(block_replica)

if __name__ == '__main__':
    # Unit test

    import pprint
    from common.inventory import InventoryManager

    logger.setLevel(logging.DEBUG)

    inventory = InventoryManager()
    locks = WebReplicaLockInterface()

    locks.update(inventory)

    pprint.pprint(locks.locked_blocks)
