import time
import datetime
import collections
import fnmatch
import logging

from common.dataformat import Dataset, DatasetReplica, Site
import common.configuration as config
import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand, history):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

        self.policies = {}

    def set_policy(self, policy): # empty partition name -> default
        self.policies[policy.partition] = policy

    def run(self, partition, is_test = False, comment = '', auto_approval = True):
        """
        1. Update the inventory if necessary.
        2. Update popularity.
        3. Create new replicas representing copy operations that should take place.
        4. Execute copy.
        """

        logger.info('Dealer run for %s starting at %s', partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))
        
        self.demand_manager.update(self.inventory_manager, accesses = False, requests = True, locks = False)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        policy = self.policies[partition]

        run_number = self.history.new_copy_run(partition.name, policy.version, is_test = is_test, comment = comment)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, self.inventory_manager)
        self.history.save_datasets(run_number, self.inventory_manager)

        target_sites = []
        for site in self.inventory_manager.sites.values():
            for pattern in dealer_config.included_sites:
                if fnmatch.fnmatch(site.name, pattern):
                    break
            else:
                continue

            if site.status == Site.STAT_READY and site.active == Site.ACT_AVAILABLE:
                target_sites.append(site)

        pending_volumes = collections.defaultdict(float)

# We have no regular updates of transfer status yet. Commenting out for now.
#        incomplete_copies = self.history.get_incomplete_copies(partition)
#        for operation in incomplete_copies:
#            site = self.inventory_manager.sites[operation.site_name]
#            status = self.transaction_manager.copy.copy_status(operation.operation_id)
#            for (site_name, dataset_name), (total, copied, update_time) in status.items():
#                if total == 0.:
#                    pending_volumes[site] += self.inventory_manager.datasets[dataset_name].size() * 1.e-12
#                else:
#                    pending_volumes[site] += (total - copied) * 1.e-12

        # all datasets that the policy considers
        datasets = []
        for dataset in self.inventory_manager.datasets.values():
            if dataset.demand.request_weight <= 0.:
                continue

            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if partition(block_replica):
                        break
                else:
                    # no block replica in partition
                    continue

                # this replica is (partially) in partition
                datasets.append(dataset)
                break

        datasets.sort(key = lambda dataset: dataset.demand.request_weight, reverse = True)

        self.history.save_dataset_popularity(run_number, datasets)

        copy_list = self.determine_copies(target_sites, datasets, policy, pending_volumes)

        logger.info('Committing copy.')
        self.commit_copies(run_number, policy, copy_list, is_test, comment, auto_approval)

        self.history.close_copy_run(run_number)

        logger.info('Finished dealer run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_copies(self, sites, datasets, policy, pending_volumes):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.
        """
        
        copy_list = dict([(site, []) for site in sites]) # site -> [new_replica]

        def compute_site_business(site):
            business = 0.
    
            for replica in list(site.dataset_replicas):
                dataset = replica.dataset
                if dataset.demand.request_weight > 0.:
                    # total capability of the sites this dataset is at
                    total_cpu = sum([r.site.cpu for r in dataset.replicas])
                    # w * N * (site cpu / total cpu); normalized by site cpu
                    business += dataset.demand.request_weight * dataset.num_files() / total_cpu

            return business

        # request-weighted, cpu-normalized number of running jobs at sites
        site_business = {}
        site_occupancy = {}
        for site in sites:
            # At the moment we don't have the information of exactly how many jobs are running at each site.
            # Assume fair share of jobs among sites: (Nreq * Nfile) * (site_capability / sum_{site}(capability))
            # jobs at each site. Normalize this by the capability at each site.

            site_business[site] = compute_site_business(site)
            site_occupancy[site] = site.storage_occupancy(policy.partition, physical = False)

        # now go through datasets sorted by weight / #replicas
        for dataset in datasets:

            dataset_size = dataset.size() * 1.e-12

            if dataset_size > dealer_config.max_dataset_size:
                continue

            if len(dataset.replicas) > dealer_config.max_replicas:
                continue

            global_stop = False

            while dataset.demand.request_weight / len(dataset.replicas) > dealer_config.request_to_replica_threshold:
                sorted_sites = sorted(site_business.items(), key = lambda (s, n): n) #sorted from emptiest to busiest

                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        dest.partition_quota(policy.partition) != 0 and \
                        site_occupancy[dest] + dataset_size / dest.partition_quota(policy.partition) < 1. and \
                        site_occupancy[dest] < dealer_config.target_site_occupancy * dealer_config.overflow_factor and \
                        pending_volumes[dest] < dealer_config.max_copy_per_site and \
                        dest.find_dataset_replica(dataset) is None
                    )

                except StopIteration:
                    logger.warning('%s has no copy destination.', dataset.name)
                    break

                logger.info('Copying %s to %s', dataset.name, destination_site.name)

                new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, policy.group)

                copy_list[destination_site].append(new_replica)

                pending_volumes[destination_site] += dataset_size
    
                # recompute site properties
                site_occupancy[destination_site] = site.storage_occupancy(policy.partition, physical = False)

                for replica in dataset.replicas:
                    site = replica.site
                    if site in site_business:
                        site_business[site] = compute_site_business(site)

                # check if we should stop copying
                if len(dataset.replicas) > dealer_config.max_replicas:
                    logger.warning('%s has reached the maximum number of replicas allowed.', dataset.name)
                    break
    
                if min(site_occupancy.values()) > dealer_config.target_site_occupancy:
                    logger.warning('All sites have exceeded target storage occupancy. No more copies will be made.')
                    global_stop = True
                    break
    
                if min(pending_volumes.values()) > dealer_config.max_copy_per_site:
                    logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                    global_stop = True
                    break

                if sum(pending_volumes.values()) > dealer_config.max_copy_total:
                    logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                    global_stop = True
                    break

            if global_stop:
                break

        return copy_list

    def commit_copies(self, run_number, policy, copy_list, is_test, comment, auto_approval):
        if not comment:
            comment = 'Dynamo -- Automatic replication request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        for site, replicas in copy_list.items():
            if len(replicas) == 0:
                continue

            for replica in list(replicas):
                if self.inventory_manager.replica_source.replica_exists_at_site(site, replica):
                    logger.info('Not copying replica because it exists at site: %s', str(replica))
                    replicas.remove(replica)

            copy_mapping = self.transaction_manager.copy.schedule_copies(replicas, policy.group, comments = comment, auto_approval = auto_approval, is_test = is_test)
            # copy_mapping .. {operation_id: (approved, [replica])}
    
            for operation_id, (approved, op_replicas) in copy_mapping.items():
                if approved and not is_test:
                    self.inventory_manager.store.add_datasetreplicas(op_replicas)
    
                size = sum([r.size(physical = False) for r in op_replicas]) # this is not group size but the total size on disk

                self.history.make_copy_entry(run_number, site, operation_id, approved, [r.dataset for r in op_replicas], size)


if __name__ == '__main__':

    import sys
    import fnmatch
    import re
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from dealer.policy import DealerPolicy

    parser = ArgumentParser(description = 'Use dealer to copy a specific dataset from a specific site.')

    parser.add_argument('replica', metavar = 'SITE:DATASET', help = 'Replica to delete.')
    parser.add_argument('--partition', '-g', metavar = 'PARTITION', dest = 'partition', default = 'AnalysisOps', help = 'Partition name.')
    parser.add_argument('--dry-run', '-D', action = 'store_true', dest = 'dry_run',  help = 'Dry run (no write / delete at all)')
    parser.add_argument('--production-run', '-P', action = 'store_true', dest = 'production_run', help = 'This is not a test.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    site_pattern, sep, dataset_pattern = args.replica.partition(':')
    
    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')
    
    kwd = {}
    for cls in ['store', 'site_source', 'dataset_source', 'replica_source']:
        kwd[cls + '_cls'] = classes.default_interface[cls]
    
    inventory_manager = InventoryManager(**kwd)
    
    transaction_manager = TransactionManager()
    
    demand_manager = DemandManager()

    history = classes.default_interface['history']()

    dealer = Dealer(inventory_manager, transaction_manager, demand_manager, history)

    # create a Partition object that allows direct manipulation of specific replicas.

    partition = inventory_manager.partitions[args.partition]

    site_re = re.compile(fnmatch.translate(site_pattern))
    dataset_re = re.compile(fnmatch.translate(dataset_pattern))

    partition_name = args.partition + ':' + args.replica

    Site.add_partition(partition_name, lambda replica: site_re.match(replica.site.name) and dataset_re.match(replica.dataset.name))

    policy = DealerPolicy(Site.partitions[partition_name])

    dealer.set_policy(policy)

    if args.dry_run:
        config.read_only = True

    dealer.run(policy.partition, is_test = not args.production_run)
