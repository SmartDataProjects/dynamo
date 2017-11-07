import logging
import fnmatch

import common.configuration as config
from common.interface.mysql import MySQL
from common.dataformat import Block

logger = logging.getLogger(__name__)

class MySQLReplicaLock(object):
    """
    A plugin for DemandManager that appends lists of block replicas that are locked.
    Sets one demand value:
      locked_blocks:   {site: set of blocks}
    """

    def __init__(self, db_params = config.registry.db_params):
        self._mysql = MySQL(**db_params)

    def load(self, inventory):
        self.update(inventory)

    def update(self, inventory):
        query = 'SELECT `item`, `sites`, `groups` FROM `detox_locks` WHERE `unlock_date` IS NULL'
        if len(config.mysqllock.users) != 0:
            query += ' AND (`user_id`, `service_id`) IN ('
            query += 'SELECT u.`id`, s.`id` FROM `users` AS u, `services` AS s WHERE '
            query += ' OR '.join('(u.`name` LIKE "%s" AND s.`name` LIKE "%s")' % us for us in config.mysqllock.users)
            query += ')'

        entries = self._mysql.query(query)

        for item_name, sites_pattern, groups_pattern in entries:
            if '#' in item_name:
                dataset_pattern, block_pattern = item_name.split('#')
            else:
                dataset_pattern = item_name
                block_pattern = None

            if '*' in dataset_pattern:
                datasets = []
                for dataset in inventory.datasets.values():
                    # this is highly inefficient but I can't think of a better way
                    if fnmatch.fnmatch(dataset.name, dataset_pattern):
                        datasets.append(dataset)
            else:
                try:
                    dataset = inventory.datasets[dataset_pattern]
                except KeyError:
                    logger.debug('Cannot lock unknown dataset %s', dataset_pattern)
                    continue

                datasets = [dataset]

            dataset_blocks = []
            for dataset in datasets:
                if block_pattern is None:
                    blocks = set(dataset.blocks)

                elif '*' in block_pattern:
                    blocks = set()
                    for block in dataset.blocks:
                        if fnmatch.fnmatch(block.real_name(), block_pattern):
                            blocks.add(block)

                else:
                    block = dataset.find_block(Block.translate_name(block_pattern))
                    if block is None:
                        logger.debug('Cannot lock unknown block %s#%s', dataset_pattern, block_pattern)
                        continue
                    
                    blocks = set([block])

                dataset_blocks.append((dataset, blocks))

            specified_sites = []
            if sites_pattern:
                if '*' in sites_pattern:
                    specified_sites.extend(s for n, s in inventory.sites.items() if fnmatch.fnmatch(n, sites_pattern))
                else:
                    try:
                        specified_sites.append(inventory.sites[sites_pattern])
                    except KeyError:
                        pass

            specified_groups = []
            if groups_pattern:
                if '*' in groups_pattern:
                    specified_groups.extend(g for n, g in inventory.groups.items() if fnmatch.fnmatch(n, groups_pattern))
                else:
                    try:
                        specified_groups.append(inventory.groups[groups_pattern])
                    except KeyError:
                        pass

            for dataset, blocks in dataset_blocks:
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
                    locked_blocks = dataset.demand['locked_blocks']
                except KeyError:
                    locked_blocks = dataset.demand['locked_blocks'] = {}
    
                for replica in dataset.replicas:
                    if replica.site not in sites:
                        continue
    
                    if replica.site not in locked_blocks:
                        locked_blocks[replica.site] = set()
    
                    for block_replica in replica.block_replicas:
                        if block_replica.group not in groups:
                            continue
    
                        if block_replica.block in blocks:
                            locked_blocks[replica.site].add(block_replica.block)


if __name__ == '__main__':
    # Unit test

    import pprint
    from common.inventory import InventoryManager

    logger.setLevel(logging.DEBUG)

    inventory = InventoryManager()
    locks = MySQLReplicaLock()

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
