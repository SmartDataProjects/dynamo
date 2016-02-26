import time

import common.configuration as config

class Detox(object):
    def __init__(self, inventory, transaction, demand):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand

    def run(self):
        if self.inventory_manager.inventory.last_update - time.time() > config.inventory.refresh_min:
            # inventory is stale -> update
            self.inventory_manager.update()

        # determine datasets to delete using demand_manager
        
        # submit deletion request through transaction_manager
