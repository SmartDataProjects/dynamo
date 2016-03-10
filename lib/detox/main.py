import time
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

class Detox(object):
    def __init__(self, inventory, transaction, demand, policy):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.policy_manager = policy

    def run(self):
        logger.info('Detox run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.inventory.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.inventory.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager.inventory)

        # loop through partial datasets and delete what can be deleted
        logger.info('Starting loop over partial dataset replicas.')
        for dataset in self.inventory_manager.datasets.values():
            iR = 0
            while iR < len(dataset.replicas):
                repl = dataset.replicas[iR]

                if not repl.is_partial:
                    iR += 1
                    continue

                logger.info('Dataset %s is partial at %s. Checking for deletion.', dataset.name, repl.site.name)

                to_delete = self.policy_manager.decision(repl, self.demand_manager.get_demand(dataset))
                if to_delete:
                    self.transaction_manager.delete(repl)
                    self.inventory_manager.delete_datasetreplica(repl)
                    # replica is taken out of dataset -> no need to increment iR

                else:
                    iR += 1

        # rank all complete datasets
        # keep removing until you hit the end of the list or all sites are within limits

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))
