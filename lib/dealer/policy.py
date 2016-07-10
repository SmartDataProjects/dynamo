import logging
import collections

import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Policy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, group, quotas, partition = ''):
        self.group = group
        self.quotas = quotas # {site: quota}
        self.partition = partition

    def applies(self, replica):
        return replica.effective_owner() == self.group

    def compute_site_business(self, site, inventory, demands):
        """
        At the moment we don't have the information of exactly how many jobs are running at each site.
        Assume fair share of jobs among sites: (Nreq * Nfile) * (site_capability / sum_{site}(capability))
        jobs at each site. Normalize this by the capability at each site.
        """

        business = 0.

        for replica in site.dataset_replicas:
            dataset = replica.dataset
            demand = demands.dataset_demands[dataset]

            if demand.request_weight > 0.:
                # total capability of the sites this dataset is at
                total_cpu = sum([r.site.cpu for r in dataset.replicas])
                # w * N * (site cpu / total cpu); normalized by site cpu
                business += demand.request_weight * dataset.num_files / total_cpu

        return business

    def compute_site_occupancy(self, site, inventory):
        """
        Site storage usage fraction for the partition. Currently partition = group.
        Take the projected (after all transfers are complete) occupancy, rather than the current physical usage.
        """

        group = inventory.groups[self.partition]
        return site.storage_occupancy(group, physical = False)

    def site_occupancy_increase(self, site, dataset, inventory):
        """
        Compute site occupancy increase by adding a dataset to site.
        """

        quota = site.group_quota[group]
        if quota == 0:
            return 0
        
        group = inventory.groups[self.partition]
        return dataset.size * 1.e-12 / quota

    def sort_datasets_by_demand(self, datasets, demands):
        """
        Return [(dataset, demand)] in decreasing order of demand.
        Current sorting is by request weight normalized by the number of replicas.
        """

        dataset_demand = [(dataset, demands.dataset_demands[dataset]) for dataset in datasets]

        return sorted(dataset_demand, key = lambda (ds, dm): dm.request_weight / len(ds.replicas), reverse = True)

    def need_copy(self, dataset, demand):
        return demand.request_weight / len(dataset.replicas) > dealer_config.request_to_replica_threshold
