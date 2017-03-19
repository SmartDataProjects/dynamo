import logging

from dealer.plugins._list import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config
from common.interface.mysqlhistory import MySQLHistory

logger = logging.getLogger(__name__)

class BalancingHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

        self.name = 'Balancer'
        self.history = None

    def get_requests(self, inventory, partition):
        """
        Return datasets, blocks, files, all sorted by priority.
        """

        if self.history is None:
            return [], [], []

        latest_run = self.history.get_latest_deletion_run(partition.name)
        
        logger.info('Balancing site occupancy based on the protected fractions in the latest run %d', latest_run)

        deletion_decisions = self.history.get_deletion_decisions(latest_run, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        for site in inventory.sites.values():
            quota = site.partition_quota(partition)
            if quota == 0:
                continue

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            protections = [(ds_name, size) for ds_name, size, decision in decisions if decision == 'protect']
            protected_fraction = sum(size for ds_name, size in protections) / float(quota)

            protected_fractions[site] = protected_fraction

            # sort protected datasets by size (small ones first)
            protections.sort(key = lambda x: x[1])

            last_copies[site] = []

            for ds_name, size in protections:
                if size > config.max_dataset_size:
                    # protections is ordered
                    break

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if len(dataset.replicas) == 1: # this replica has no other copy
                    logger.debug('%s is a last copy at %s', ds_name, site.name)
                    last_copies[site].append(dataset)

        request = []

        total_size = 0.
        variation = 1.
        # The actual cutoff will be imposed by Dealer later
        # Just to not make copy proposals that are never fulfilled
        while len(protected_fractions) != 0 and total_size < config.max_copy_total:
            maxsite, maxfrac = max(protected_fractions.items(), key = lambda x: x[1])
            minsite, minfrac = min(protected_fractions.items(), key = lambda x: x[1])
            
            # if max - min is less than 5%, we are done
            if maxfrac - minfrac < 0.05:
                break

            try:
                dataset = last_copies[maxsite].pop(0)
            except IndexError: # nothing to copy from this site
                protected_fractions.pop(maxsite)
                continue

            request.append((dataset, minsite))

            size = dataset.size()
            protected_fractions[maxsite] -= size / float(maxsite.partition_quota(partition))
            protected_fractions[minsite] += size / float(minsite.partition_quota(partition))

            total_size += size

        return request, [], []

    def save_record(self, run_number, history, copy_list):
        pass


plugins['Balancer'] = BalancingHandler()
