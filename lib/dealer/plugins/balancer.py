import logging
import random

from dealer.plugins.base import BaseHandler

LOG = logging.getLogger(__name__)

class BalancingHandler(BaseHandler):
    """
    Copy datasets from sites with higher protected fraction to sites with lower fraction.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Balancer')

        self.required_attrs = ['request_weight']

        self.max_dataset_size = config.max_dataset_size * 1.e+12
        self.target_reasons = dict(config.target_reasons)

    def get_requests(self, inventory, history, policy):
        latest_runs = history.get_deletion_runs(policy.partition_name)
        if len(latest_runs) == 0:
            return []

        latest_run = latest_runs[0]

        LOG.info('Balancing site occupancy based on the protected fractions in the latest cycle %d', latest_run)

        deletion_decisions = history.get_deletion_decisions(latest_run, size_only = False)

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
            protected_fraction = sum(size for ds_name, size, reason in protections) / quota

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
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if dataset.find_replica(site) is None:
                    # this replica has disappeared since then
                    continue

                for target_reason, num_rep in self.target_reasons.items():
                    if reason != target_reason:
                        continue

                    num_nonpartial = 0
                    for replica in dataset.replicas:
                        if not replica.is_partial():
                            num_nonpartial += 1

                    if num_nonpartial < num_rep:
                        LOG.debug('%s is a last copy at %s', ds_name, site.name)
                        last_copies[site].append(dataset)

        for site, frac in sorted(protected_fractions.items(), key = lambda (s, f): f):
            LOG.debug('Site %s fraction %f', site.name, frac)

        request = []

        total_size = 0
        variation = 1.
        # The actual cutoff will be imposed by Dealer later
        # This cutoff is just to not make copy proposals that are never fulfilled
        while len(protected_fractions) != 0 and total_size < policy.max_total_cycle_volume:
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
            protected_fractions[maxsite] -= size / maxsite.partitions[partition].quota
            total_size += size

        return request

    def save_record(self, run_number, history, copy_list):
        pass
