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

    def __init__(self, db_params = config.mysqllock.db_params):
        self._mysql = MySQL(**db_params)

    def load(self, inventory):
        self.update(inventory)

    def update(self, inventory):
        entries = self._mysql.query('SELECT `item`, `sites`, `groups` FROM `detox_locks` WHERE `unlock_date` IS NULL')

        for item_name, sites_pattern, groups_pattern in entries:
            if '#' in item_name:
                dataset_name, block_real_name = item_name.split('#')
            else:
                dataset_name = item_name
                block_real_name = None

            try:
                dataset = inventory.datasets[dataset_name]
            except KeyError:
                logger.debug('Cannot lock unknown dataset %s', dataset_name)
                continue

            if dataset.replicas is None:
                continue

            if dataset.blocks is None:
                inventory.store.load_blocks(dataset)

            if block_real_name is None:
                blocks = list(dataset.blocks)
            else:
                block = dataset.find_block(Block.translate_name(block_real_name))
                if block is None:
                    logger.debug('Cannot lock unknown block %s#%s', dataset_name, block_real_name)
                    continue

                blocks = [block]

            sites = set()
            if sites_pattern:
                if '*' in sites_pattern:
                    sites.update(s for n, s in inventory.sites.items() if fnmatch.fnmatch(n, sites_pattern))
                else:
                    try:
                        sites.add(inventory.sites[sites_pattern])
                    except KeyError:
                        pass

            if len(sites) == 0:
                # if no site matches the pattern, we will be on the safe side and treat it as a global lock
                sites.update(r.site for r in dataset.replicas)

            groups = set()
            if groups_pattern:
                if '*' in groups_pattern:
                    groups.update(g for n, g in inventory.groups.items() if fnmatch.fnmatch(n, groups_pattern))
                else:
                    try:
                        groups.add(inventory.groups[groups_pattern])
                    except KeyError:
                        pass

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
