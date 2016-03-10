import time

import common.configuration as config

class Detox(object):
    def __init__(self, inventory, transaction, demand, policy):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.policy_manager = policy

    def run(self):
        if self.inventory_manager.inventory.last_update - time.time() > config.inventory.refresh_min:
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager)

        # loop through partial datasets and delete what can be deleted
        for dataset in self.inventory_manager.datasets.values():
            iR = 0
            while iR < len(dataset.replicas):
                repl = dataset.replicas[iR]

                if not repl.is_partial:
                    continue

                to_delete = self.policy_manager.decision(repl, self.demand_manager.get_demand(dataset))
                if to_delete:
                    self.transaction_manager.delete(repl)
                    self.inventory_manager.delete_datasetreplica(repl)
                    # replica is taken out of dataset -> no need to increment iR

                else:
                    iR += 1

        # rank all complete datasets
        # keep removing until you hit the end of the list or all sites are within limits
