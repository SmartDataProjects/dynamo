import logging
import collections
import urllib2
import time

from common.interface.lock import ReplicaLockInterface
from common.interface.webservice import RESTService
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
        
    def add_source(self, url, content_type, data_type = 'application/json'):
        if type(content_type) is str:
            content_type = eval('WebReplicaLockInterface.' + content_type)

        self._sources.append((RESTService(url, accept = data_type), content_type))

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
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        continue

                    for dataset_replica in dataset.replicas:
                        for block_replica in dataset_replica.block_replicas:
                            self.locked_blocks[dataset].append(block_replica)

            elif content_type == WebReplicaLockInterface.CMSWEB_LIST_OF_DATASETS:
                # data['result'] -> simple list of datasets
                for dataset_name in data['result']:
                    if dataset_name is None:
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        continue

                    for dataset_replica in dataset.replicas:
                        for block_replica in dataset_replica.block_replicas:
                            self.locked_blocks[dataset].append(block_replica)
                
            elif content_type == WebReplicaLockInterface.SITE_TO_DATASETS:
                # data = {site: {dataset: info}}
                for site_name, datasets in data.items():
                    try:
                        site = inventory.sites[site_name]
                    except KeyError:
                        continue

                    for dataset_name, info in datasets.items():
                        if not info['lock']:
                            continue

                        try:
                            dataset = inventory.datasets[dataset_name]
                        except KeyError:
                            continue

                        replica = dataset.find_replica(site)
                        if replica is None:
                            continue

                        for block_replica in replica.block_replicas:
                            self.locked_blocks[dataset].append(block_replica)
