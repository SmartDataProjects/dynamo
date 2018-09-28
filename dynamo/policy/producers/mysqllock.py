import logging
import fnmatch
import re

from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration, Block, ObjectError, ConfigurationError

LOG = logging.getLogger(__name__)

class MySQLReplicaLock(object):
    """
    Dataset lock read from local DB.
    Sets one attr:
      locked_blocks:   {site: set([blocks]) or None if dataset-level}
    """

    produces = ['locked_blocks']

    _default_config = None

    @staticmethod
    def set_default(config):
        MySQLReplicaLock._default_config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            if MySQLReplicaLock._default_config is None:
                raise ConfigurationError('MySQLReplicaLock default config is not set')

            config = MySQLReplicaLock._default_config

        self._mysql = MySQL(config.get('db_params', None))

        self.users = []
        for user_id, role_id in config.get('users', []):
            self.users.append((user_id, role_id))

    def load(self, inventory):
        for dataset in inventory.datasets.itervalues():
            try:
                dataset.attr.pop('locked_blocks')
            except KeyError:
                pass

        if len(self.users) != 0:
            entries = self._mysql.select_many('detox_locks', ('item', 'sites', 'groups'), ('user_id', 'role_id'), self.users)
        else:
            query = 'SELECT `item`, `sites`, `groups` FROM `detox_locks`'
            entries = self._mysql.query(query)

        for item_name, sites_pattern, groups_pattern in entries:
            # wildcard not allowed in block name
            try:
                dataset_pattern, block_name = Block.from_full_name(item_name)
            except ObjectError:
                dataset_pattern, block_name = item_name, None

            if '*' in dataset_pattern:
                pat_exp = re.compile(fnmatch.translate(dataset_pattern))
                
                datasets = []
                for dataset in inventory.datasets.values():
                    # this is highly inefficient but I can't think of a better way
                    if pat_exp.match(dataset.name):
                        datasets.append(dataset)
            else:
                try:
                    dataset = inventory.datasets[dataset_pattern]
                except KeyError:
                    LOG.debug('Cannot lock unknown dataset %s', dataset_pattern)
                    continue

                datasets = [dataset]

            specified_sites = []
            if sites_pattern:
                if sites_pattern == '*':
                    pass
                elif '*' in sites_pattern:
                    pat_exp = re.compile(fnmatch.translate(sites_pattern))
                    specified_sites.extend(s for n, s in inventory.sites.iteritems() if pat_exp.match(n))
                else:
                    try:
                        specified_sites.append(inventory.sites[sites_pattern])
                    except KeyError:
                        pass

            specified_groups = []
            if groups_pattern:
                if groups_pattern == '*':
                    pass
                elif '*' in groups_pattern:
                    pat_exp = re.compile(fnmatch.translate(groups_pattern))
                    specified_groups.extend(g for n, g in inventory.groups.iteritems() if pat_exp.match(n))
                else:
                    try:
                        specified_groups.append(inventory.groups[groups_pattern])
                    except KeyError:
                        pass

            for dataset in datasets:
                sites = set(specified_sites)
                groups = set(specified_groups)

                if len(sites) == 0:
                    # either sites_pattern was not given (global lock) or no sites matched (typo?)
                    # we will treat this as a global lock
                    sites.update(r.site for r in dataset.replicas)
    
                if len(groups) == 0:
                    # if no group matches the pattern, we will be on the safe side and treat it as a global lock
                    for replica in dataset.replicas:
                        groups.update(brep.group for brep in replica.block_replicas)
    
                try:
                    locked_blocks = dataset.attr['locked_blocks']
                except KeyError:
                    locked_blocks = dataset.attr['locked_blocks'] = {}

                if block_name is None:
                    for replica in dataset.replicas:
                        if replica.site not in sites:
                            continue
        
                        if replica.site not in locked_blocks:
                            locked_blocks[replica.site] = set()
        
                        for block_replica in replica.block_replicas:
                            if block_replica.group not in groups:
                                continue
        
                            locked_blocks[replica.site].add(block_replica.block)
                else:
                    block = dataset.find_block(block_name)
                    if block is None:
                        LOG.debug('Cannot lock unknown block %s', block_name)
                        continue

                    for replica in block.replicas:
                        if replica.site not in sites:
                            continue

                        if replica.group not in groups:
                            continue
        
                        if replica.site not in locked_blocks:
                            locked_blocks[replica.site] = set([block])
                        else:
                            locked_blocks[replica.site].add(block)
                            
        for dataset in inventory.datasets.itervalues():
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

        LOG.info('Locked %d items.', len(entries))
