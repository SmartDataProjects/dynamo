import logging
import fnmatch
import collections

import common.configuration as config
from common.interface.lock import ReplicaLockInterface
from common.interface.mysql import MySQL
from common.dataformat import Block

logger = logging.getLogger(__name__)

class MySQLLockInterface(ReplicaLockInterface):
    """
    Implementation of ReplicaLockInterface using a DB.
    """

    def __init__(self, db_params = config.mysqllock.db_params):
        ReplicaLockInterface.__init__(self)
        
        self._mysql = MySQL(**db_params)

    def update(self, inventory): #override
        self.locked_blocks = collections.defaultdict(list)

        entries = self._mysql.query('SELECT `item`, `sites`, `groups` FROM `detox_locks` WHERE `enabled` = 1')

        for item_name, sites_pattern, groups_pattern in entries:
            if '#' in item_name:
                dataset_name, block_real_name = item_name.split('#')
                block_name = Block.translate_name(block_real_name)
            else:
                dataset_name = item_name
                block_name = 0

            try:
                dataset = inventory.datasets[dataset_name]
            except KeyError:
                logger.debug('Cannot lock unknown dataset %s', dataset_name)
                continue

            if dataset.replicas is None:
                continue

            if block_name != 0:
                block = dataset.find_block(block_name)
                if block is None:
                    logger.debug('Cannot lock unknown block %s#%s', dataset_name, block_real_name)
                    continue

                locked_blocks = [block]

            else:
                locked_blocks = list(dataset.blocks)

            sites = []
            if sites_pattern:
                # if no site matches the pattern, we will be on the safe side and treat it as a global lock
                if '*' in sites_pattern:
                    sites = [s for n, s in inventory.sites.items() if fnmatch.fnmatch(n, sites_pattern)]
                else:
                    try:
                        sites = [inventory.sites[sites_pattern]]
                    except KeyError:
                        pass

            groups = []
            if groups_pattern:
                # if no group matches the pattern, we will be on the safe side and treat it as a global lock
                if '*' in groups_pattern:
                    groups = [g for n, g in inventory.groups.items() if fnmatch.fnmatch(n, groups_pattern)]
                else:
                    try:
                        groups = [inventory.groups[groups_pattern]]
                    except KeyError:
                        pass

            for replica in dataset.replicas:
                if len(sites) != 0 and replica.site not in sites:
                    continue

                for block_replica in replica.block_replicas:
                    if len(groups) != 0 and block_replica.group not in groups:
                        continue

                    if block_replica.block in locked_blocks:
                        self.locked_blocks[dataset].append(block_replica)

if __name__ == '__main__':
    # Unit test

    import pprint
    from common.inventory import InventoryManager

    logger.setLevel(logging.DEBUG)

    inventory = InventoryManager()
    locks = MySQLLockInterface()

    locks.update(inventory)

    pprint.pprint(locks.locked_blocks)
