from dealer.plugins._list import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config
import detox.configuration as detoxconfig
from common.interface.mysqlhistory import MySQLHistory

class BalancingHandler(BaseHandler):
    def __init__(self):
        self.history = None

    def get_requests(self, inventory, partition):
        """
        Return datasets, blocks, files, all sorted by priority.
        """

        if self.history is None:
            return [], [], []

        request_datasets = []

        latest_run = self.history.get_latest_deletion_run(partition.name)
        deletion_decisions = self.history.get_deletion_decisions(latest_run, size_only = False)

        for site in inventory.sites.values():
            quota = site.partition_quota(partition)

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            protected_volume = sum(size for ds_name, size, decision in decisions if decision == 'protect')
            if protected_volume < quota * detoxconfig.threshold_occupancy:
                continue

            # sort protected datasets by size (small ones first)
            protected_volume.sort(key = lambda x: x[1])

            for ds_name, size, decision in protected_volume:
                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if len(dataset.replicas) == 1: # this replica has no other copy
                    request_datasets.append(dataset)

        return request_datasets, [], []

    def save_record(self, run_number, history, copy_list):
        pass


plugins['Balancer'] = BalancingHandler()
