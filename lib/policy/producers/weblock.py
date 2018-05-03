import logging
import collections
import urllib2
import fnmatch
import re
import time

import dynamo.utils.interface.webservice as webservice
from dynamo.dataformat import Configuration, Block, ObjectError

LOG = logging.getLogger(__name__)

class WebReplicaLock(object):
    """
    Dataset lock read from www sources.
    Sets one attr:
      locked_blocks:   {site: set([blocks]) or None if dataset-level}
    """

    produces = ['locked_blocks']

    # content types
    LIST_OF_DATASETS, CMSWEB_LIST_OF_DATASETS, SITE_TO_DATASETS = range(3)

    def __init__(self, config):
        self._sources = {} # {name: (RESTService, content type, site pattern, lock of locks)}

        for name, source_config in config.sources.items():
            self.add_source(name, source_config, config.auth)

    def add_source(self, name, config, auth_config):
        rest_config = Configuration()
        rest_config.url_base = config.url
        rest_config.accept = config.get('data_type', 'application/json')
        if config.auth == 'noauth':
            rest_config.auth_handler = 'None'
        else:
            auth = auth_config[config.auth]
            rest_config.auth_handler = auth.auth_handler
            rest_config.auth_handler_conf = Configuration(auth.get('auth_handler_conf', {}))

        content_type = getattr(WebReplicaLock, config.content_type)
        site_pattern = config.get('sites', None)
        lock_url = config.get('lock_url', None)

        self._sources[name] = (webservice.RESTService(rest_config), content_type, site_pattern, lock_url)

    def load(self, inventory):
        for dataset in inventory.datasets.itervalues():
            try:
                dataset.attr.pop('locked_blocks')
            except KeyError:
                pass

        for item, site in self.get_list(inventory):
            if type(item) is Dataset:
                dataset = item
                block = None
            else:
                dataset = item.dataset
                block = item

            try:
                locked_blocks = dataset.attr['locked_blocks']
            except KeyError:
                locked_blocks = dataset.attr['locked_blocks'] = {}

            if site is None:
                sites = [r.site for r in dataset.replicas]
            else:
                sites = [site]
                    
            for st in sites:
                if block is None:
                    locked_blocks[st] = None
                elif st in locked_blocks:
                    if locked_blocks[st] is None:
                        pass
                    else:
                        locked_blocks[st].add(block)
                else:
                    locked_blocks[st] = set(block)

        for dataset in inventory.dataests.itervalues():
            try:
                locked_blocks = dataset.attr['locked_blocks']
            except KeyError:
                continue

            for site, blocks in locked_blocks.items():
                if blocks is None:
                    continue

                # if all blocks are locked, set to None (dataset-level lock)
                if blocks == dataset.blocks:
                    locked_blocks[site] = None

    def get_list(self, inventory):
        all_locks = [] # [(item, site)]

        for source, content_type, site_pattern, lock_url in self._sources.itervalues():        
            if lock_url is not None:
                # check that the lock files themselves are not locked
                while True:
                    # Hacky but this is temporary any way
                    opener = urllib2.build_opener(webservice.HTTPSCertKeyHandler(Configuration()))
                    opener.addheaders.append(('Accept', 'application/json'))
                    request = urllib2.Request(lock_url)
                    try:
                        opener.open(request)
                    except urllib2.HTTPError as err:
                        if err.code == 404:
                            # file not found -> no lock
                            break
                        else:
                            raise
        
                    LOG.info('Lock files are being produced. Waiting 60 seconds.')
                    time.sleep(60)

            if site_pattern is None:
                site_re = None
            else:
                site_re = re.compile(fnmatch.translate(site_pattern))

            LOG.info('Retrieving lock information from %s', source.url_base)

            data = source.make_request()

            if content_type == WebReplicaLock.LIST_OF_DATASETS:
                # simple list of datasets
                for dataset_name in data:
                    if dataset_name is None:
                        LOG.debug('Dataset name None found in %s', source.url_base)
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        LOG.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                        continue

                    if site_re is not None:
                        for replica in dataset.replicas:
                            if not site_re.match(replica.site.name):
                                continue
                            
                            all_locks.append((dataset, replica.site))
                    else:
                        all_locks.append((dataset, None))

            elif content_type == WebReplicaLock.CMSWEB_LIST_OF_DATASETS:
                # data['result'] -> simple list of datasets
                for dataset_name in data['result']:
                    if dataset_name is None:
                        LOG.debug('Dataset name None found in %s', source.url_base)
                        continue

                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        LOG.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                        continue

                    if site_re is not None:
                        for replica in dataset.replicas:
                            if not site_re.match(replica.site.name):
                                continue
    
                            all_locks.append((dataset, replica.site))
                    else:
                        all_locks.append((dataset, None))
                
            elif content_type == WebReplicaLock.SITE_TO_DATASETS:
                # data = {site: {dataset: info}}
                for site_name, objects in data.items():
                    try:
                        site = inventory.sites[site_name]
                    except KeyError:
                        LOG.debug('Unknown site %s in %s', site_name, source.url_base)
                        continue

                    for object_name, info in objects.items():
                        if not info['lock']:
                            LOG.debug('Object %s is not locked at %s', object_name, site_name)
                            continue

                        try:
                            dataset_name, block_name = Block.from_full_name(object_name)
                        except ObjectError:
                            dataset_name, block_name = object_name, None

                        try:
                            dataset = inventory.datasets[dataset_name]
                        except KeyError:
                            LOG.debug('Unknown dataset %s in %s', dataset_name, source.url_base)
                            continue

                        replica = site.find_dataset_replica(dataset)
                        if replica is None:
                            LOG.debug('Replica of %s is not at %s in %s', dataset_name, site_name, source.url_base)
                            continue

                        if block_name is None:
                            all_locks.append((dataset, site))
                        else:
                            block_replica = replica.find_block_replica(block_name)
                            if block_replica is None:
                                LOG.debug('Unknown block %s in %s', object_name, source.url_base)
                                continue

                            all_locks.append((block_replica.block, site))

        return all_locks
