import logging
import fnmatch
import re

from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Dataset, Block, ObjectError

LOG = logging.getLogger(__name__)

class MySQLReplicaLock(object):
    """
    Dataset lock read from local DB.
    Sets one attr:
      locked_blocks:   {site: set([blocks]) or None if dataset-level}
    """

    produces = ['locked_blocks']

    def __init__(self, config):
        self._mysql = MySQL(config.get('db_params', None))

    def load(self, inventory):
        entries = 0
        for item_name, site_name in self._mysql.xquery('SELECT `item`, `site` FROM `detox_locked_replicas`'):
            try:
                dataset_name, block_name = Block.from_full_name(item_name)
            except ObjectError:
                dataset_name, block_name = item_name, None

            try:
                dataset = inventory.datasets[dataset_name]
            except KeyError:
                LOG.debug('Cannot lock unknown dataset %s', dataset_name)
                continue

            try:
                site = inventory.sites[site_name]
            except KeyError:
                LOG.debug('Cannot lock at unknown site %s', site_name)
                continue

            if block_name is None:
                replica = site.find_dataset_replica(dataset)
                if replicas is None:
                    LOG.debug('Cannot lock nonexistent replica %s:%s', site_name, dataset_name)
                    continue

                try:
                    dataset.attr['locked_blocks'][site] = None
                except KeyError:
                    dataset.attr['locked_blocks'] = {site: None}

            else:
                block = dataset.find_block(block_name)
                if block is None:
                    LOG.debug('Cannot lock unknown block %s', item_name)
                    continue
                
                try:
                    locked_blocks = dataset.attr['locked_blocks']
                except KeyError:
                    locked_blocks = dataset.attr['locked_blocks'] = {}

                try:
                    locked_blocks[site].add(block)
                except KeyError:
                    locked_blocks[site] = set([block])
                except AttributeError:
                    #locked_blocks[site] was set but was None
                    pass

            entries += 1

        LOG.info('Locked %d items.', len(entries))


    def lock(self, item, site):
        """
        Lock an item.
        @param item   Dataset or Block.
        @param site   Site
        """

        if type(item) is Dataset:
            item_name = item.name
        else:
            item_name = item.full_name()

        self._mysql.insert_update('detox_locked_replicas', ('item', 'site'), item_name, site.name)

    def unlock(self, item, site):
        """
        Unlock an item.
        @param item   Dataset or Block.
        @param site   Site
        """

        if type(item) is Dataset:
            item_name = item.name
        else:
            item_name = item.full_name()

        self._mysql.query('DELETE FROM `detox_locked_replicas` WHERE `item` = %s AND `site` = %s')
