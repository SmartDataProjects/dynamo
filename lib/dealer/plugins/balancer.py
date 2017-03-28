import logging

from dealer.plugins import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config
from common.interface.mysqlhistory import MySQLHistory
from common.dataformat import Site

logger = logging.getLogger(__name__)

class BalancingHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

        self.name = 'Balancer'
        self.history = None

    def get_requests(self, inventory, partition):
        if self.history is None:
            return [], [], []

        latest_run = self.history.get_latest_deletion_run(partition.name)
        
        logger.info('Balancing site occupancy based on the protected fractions in the latest run %d', latest_run)

        deletion_decisions = self.history.get_deletion_decisions(latest_run, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        for site in inventory.sites.values():
            quota = site.partition_quota(partition)

            # do not consider bad sites in balancing in any way (it's not this plugin's job to offload protected data from bad sites)
            if quota == 0. or site.status != Site.STAT_READY or site.active != Site.ACT_AVAILABLE:
                continue

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            protections = [(ds_name, size) for ds_name, size, decision in decisions if decision == 'protect']
            protected_fraction = sum(size for ds_name, size in protections) * 1.e-12 / quota

            logger.debug('Site %s protected fraction %f', site.name, protected_fraction)

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
        # This cutoff is just to not make copy proposals that are never fulfilled
        while len(protected_fractions) != 0 and total_size < config.max_copy_total:
            maxsite, maxfrac = max(protected_fractions.items(), key = lambda x: x[1])
            minsite, minfrac = min(protected_fractions.items(), key = lambda x: x[1])

            logger.debug('Protected fraction variation %f', maxfrac - minfrac)
            
            # if max - min is less than 5%, we are done
            if maxfrac - minfrac < 0.05:
                break

            try:
                dataset = last_copies[maxsite].pop(0)
            except IndexError: # nothing to copy from this site
                protected_fractions.pop(maxsite)
                continue

            logger.debug('Proposing to copy %s to %s', dataset.name, minsite.name)

            request.append((dataset, minsite))

            size = dataset.size() * 1.e-12
            protected_fractions[maxsite] -= size / float(maxsite.partition_quota(partition))
            protected_fractions[minsite] += size / float(minsite.partition_quota(partition))

            total_size += size

        return request, [], []

    def save_record(self, run_number, history, copy_list):
        pass


plugins['Balancer'] = BalancingHandler()
