import time
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand

        self.copy_message = 'DynaMO -- Automatic Replication Request.'

    def run(self):
        """
        1. Update the inventory if necessary.
        2. Update popularity.
        3. Create pairs of (new replica, source) representing copy operations that should take place.
        4. Execute copy.
        """
        
        logger.info('Dealer run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager)

        copy_list = self.determine_copies()

        for replica, origin in copy_list:
            self.transaction_manager.copy.schedule_copy(replica.dataset, replica.site, origin, comments = self.copy_message)

    def determine_copies(self):
        """
        Simplest algorithm:
        1. Sort datasets by time- and slot-
        """
