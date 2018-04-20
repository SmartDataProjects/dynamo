import logging
import random

from base import BaseHandler
from dynamo.dataformat import Site
from dynamo.detox.history import DetoxHistory

LOG = logging.getLogger(__name__)

class BalancingHandler(BaseHandler):
    """
    Copy datasets from sites with higher protected fraction to sites with lower fraction.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Balancer')

        self.max_dataset_size = config.max_dataset_size * 1.e+12
        self.max_cycle_volume = config.max_cycle_volume * 1.e+12
        self.detoxhistory = DetoxHistory(config.detox_history)
        self.target_reasons = dict(config.target_reasons)

    def get_requests(self, inventory, history, policy):
        latest_cycles = history.get_deletion_cycles(policy.partition_name)
        if len(latest_cycles) == 0:
            return []

        latest_cycle = latest_cycles[0]

        LOG.info('Balancing site occupancy based on the protected fractions in the latest cycle %d', latest_cycle)
        LOG.debug('Protection reason considered as "last copy":')
        for reason in self.target_reasons.keys():
            LOG.debug(reason)

        deletion_decisions = self.detoxhistory.get_deletion_decisions(latest_cycle, size_only = False)

        partition = inventory.partitions[policy.partition_name]

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        for site in inventory.sites.values():
            quota = site.partitions[partition].quota

            LOG.debug('Site %s quota %f TB', site.name, quota * 1.e-12)

            if quota <= 0:
                # if the site has 0 or infinite quota, don't consider in balancer
                continue

            LOG.debug('Site %s in deletion_decisions %d', site.name, (site.name in deletion_decisions))

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            protections = [(ds_name, size, reason) for ds_name, size, decision, reason in decisions if decision == 'protect']
            protected_fraction = float(sum(size for _, size, _ in protections)) / quota

            LOG.debug('Site %s protected fraction %f', site.name, protected_fraction)

            protected_fractions[site] = protected_fraction

            # sort protected datasets by size (small ones first)
            protections.sort(key = lambda x: x[1])

            last_copies[site] = []

            for ds_name, size, reason in protections:
                if size > self.max_dataset_size:
                    # protections is ordered -> there are no more
                    break

                try:
                    num_rep = self.target_reasons[reason]
                except KeyError:
                    # protected not because it was the last copy
                    continue

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if dataset.find_replica(site) is None:
                    # this replica has disappeared since then
                    continue

                num_nonpartial = 0
                for replica in dataset.replicas:
                    if replica.site.storage_type == Site.TYPE_MSS:
                        continue

                    if replica.is_partial():
                        continue

                    if replica in replica.site.partitions[partition].replicas:
                        num_nonpartial += 1

                if num_nonpartial <= num_rep:
                    LOG.debug('%s is a last copy at %s', ds_name, site.name)
                    last_copies[site].append(dataset)

        for site, frac in sorted(protected_fractions.items(), key = lambda (s, f): f):
            LOG.debug('Site %s fraction %f', site.name, frac)

        request = []

        total_size = 0
        variation = 1.

        while len(protected_fractions) != 0 and total_size < self.max_cycle_volume:
            maxsite, maxfrac = max(protected_fractions.items(), key = lambda x: x[1])
            minsite, minfrac = min(protected_fractions.items(), key = lambda x: x[1])

            LOG.debug('Protected fraction variation %f', maxfrac - minfrac)
            LOG.debug('Max site: %s', maxsite.name)

            # if max - min is less than 5%, we are done
            if maxfrac - minfrac < 0.05:
                break

            try:
                dataset = last_copies[maxsite].pop(0)
            except IndexError: # nothing to copy from this site
                protected_fractions.pop(maxsite)
                continue

            request.append(dataset)

            size = dataset.size
            protected_fractions[maxsite] -= float(size) / maxsite.partitions[partition].quota
            total_size += size

        return request
