import logging
import random

from dealer.plugins import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config
from common.interface.mysqlhistory import MySQLHistory
from common.dataformat import Site

logger = logging.getLogger(__name__)

class BalancingHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self, 'Balancer')
        self.history = None

    def get_requests(self, inventory, policy):
        if self.history is None:
            return []

        latest_runs = self.history.get_deletion_runs(policy.partition.name)
        if len(latest_runs) == 0:
            return []

        latest_run = latest_runs[0]

        logger.info('Balancing site occupancy based on the protected fractions in the latest cycle %d', latest_run)

        deletion_decisions = self.history.get_deletion_decisions(latest_run, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        for site in inventory.sites.values():
            quota = site.partition_quota(policy.partition)

            logger.debug('Site %s quota %f', site.name, quota)

            if quota <= 0:
                # if the site has 0 or infinite quota, don't consider in balancer
                continue

            logger.debug('Site %s in deletion_decisions %d', site.name, (site.name in deletion_decisions))

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            protections = [(ds_name, size, reason) for ds_name, size, decision, reason in decisions if decision == 'protect']
            protected_fraction = sum(size for ds_name, size, reason in protections) * 1.e-12 / quota

            logger.debug('Site %s protected fraction %f', site.name, protected_fraction)

            protected_fractions[site] = protected_fraction

            # sort protected datasets by size (small ones first)
            protections.sort(key = lambda x: x[1])

            last_copies[site] = []

            for ds_name, size, reason in protections:
                if size * 1.e-12 > config.main.max_dataset_size:
                    # protections is ordered
                    break

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if dataset.find_replica(site) is None:
                    # this replica has disappeared since then
                    continue

                for target_reason, num_rep in config.balancer.target_reasons:
                    if reason != target_reason:
                        continue

                    num_nonpartial = 0
                    for replica in dataset.replicas:
                        if not replica.is_partial():
                            num_nonpartial += 1

                    if num_nonpartial < num_rep:
                        logger.debug('%s is a last copy at %s', ds_name, site.name)
                        last_copies[site].append(dataset)

        for site, frac in sorted(protected_fractions.items(), key = lambda (s, f): f):
            logger.debug('Site %s fraction %f', site.name, frac)

        request = []

        total_size = 0.
        variation = 1.
        # The actual cutoff will be imposed by Dealer later
        # This cutoff is just to not make copy proposals that are never fulfilled
        while len(protected_fractions) != 0 and total_size < config.main.max_copy_total:
            maxsite, maxfrac = max(protected_fractions.items(), key = lambda x: x[1])
            minsite, minfrac = min(protected_fractions.items(), key = lambda x: x[1])

            logger.debug('Protected fraction variation %f', maxfrac - minfrac)
            logger.debug('Max site: %s', maxsite.name)

            # if max - min is less than 5%, we are done
            if maxfrac - minfrac < 0.05:
                break

            try:
                dataset = last_copies[maxsite].pop(0)
            except IndexError: # nothing to copy from this site
                protected_fractions.pop(maxsite)
                continue

            request.append(dataset)

            size = dataset.size * 1.e-12
            protected_fractions[maxsite] -= size / float(maxsite.partition_quota(policy.partition))

            total_size += size

        return request

    def save_record(self, run_number, history, copy_list):
        pass


plugins['Balancer'] = BalancingHandler()
