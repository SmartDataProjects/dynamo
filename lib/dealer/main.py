import time
import datetime
import collections
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

        self.copy_message = 'Dynamo -- Automatic Replication Request.'

    def set_policy(self, policy, partition = ''): # empty partition name -> default
        self.policies[partition] = policy

    def run(self, partition = '', is_test = False):
        """
        1. Update the inventory if necessary.
        2. Update popularity.
        3. Create new replicas representing copy operations that should take place.
        4. Execute copy.
        """

        if partition:
            logger.info('Dealer run for %s starting at %s', partition, time.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            logger.info('Dealer run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))
        
        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager, accesses = False, requests = True)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        policy = self.policies[partition]

        run_number = self.history.new_copy_run(partition, is_test = is_test)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, self.inventory_manager)
        self.history.save_datasets(run_number, self.inventory_manager)
        # take a snapshot of current replicas
        self.history.save_replicas(run_number, self.inventory_manager)
        # take snapshots of quotas if updated
        self.history.save_quotas(run_number, partition, policy.quotas, self.inventory_manager)

        incomplete_copies = self.history.get_incomplete_copies(partition)
        copy_volumes = collections.defaultdict(float)
        for operation in incomplete_copies:
            site = self.inventory_manager.sites[operation.site_name]
            status = self.transaction_manager.copy.copy_status(operation.operation_id)
            for (site_name, dataset_name), (total, copied) in status.items():
                if total == 0.:
                    copy_volumes[site] += self.inventory_manager.datasets[dataset_name].size * 1.e-12
                else:
                    copy_volumes[site] += (total - copied) * 1.e-12

        # all datasets that the policy considers
        datasets = []
        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                if policy.applies(replica):
                    datasets.append(dataset)
                    break

        copy_list = self.determine_copies_by_requests(datasets, policy, copy_volumes)

#        self.history.save_copy_decisions(run_number, copy_list)

        logger.info('Committing copy.')
        self.commit_copies(run_number, copy_list, is_test)

        self.history.close_copy_run(run_number)

        logger.info('Finished dealer run at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_copies_by_requests(self, datasets, policy, copy_volumes):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.
        """
        
        sites = self.inventory_manager.sites.values()
        
        copy_list = dict([(site, []) for site in sites]) # site -> [new_replica]

        # request-weighted, cpu-normalized number of running jobs at sites
        site_business = {}
        site_occupancy = {}
        for site in sites:
            site_business[site] = policy.compute_site_business(site, self.inventory_manager, self.demand_manager)
            site_occupancy[site] = policy.compute_site_occupancy(site, self.inventory_manager)

        sorted_datasets = policy.sort_datasets_by_demand(datasets, self.demand_manager)
        
        # now go through datasets sorted by weight / #replicas
        for dataset, demand in sorted_datasets:

            if dataset.size * 1.e-12 > dealer_config.max_dataset_size:
                continue

            if len(dataset.replicas) > dealer_config.max_replicas:
                continue

            global_stop = False

            while policy.need_copy(dataset, demand):
                sorted_sites = sorted(site_business.items(), key = lambda (s, n): n) #sorted from emptiest to busiest

                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        dest.status == Site.STAT_READY and \
                        dest.name not in dealer_config.excluded_destinations and \
                        site_occupancy[dest] < config.target_site_occupancy * dealer_config.overflow_factor and \
                        copy_volumes[dest] < dealer_config.max_copy_per_site and \
                        dest.find_dataset_replica(dataset) is None
                    )

                except StopIteration:
                    logger.warning('%s has no copy destination.', dataset.name)
                    break

                logger.info('Copying %s to %s', dataset.name, destination_site.name)

                new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, policy.group)

                copy_list[destination_site].append(new_replica)

                copy_volumes[destination_site] += dataset.size * 1.e-12
    
                # recompute site properties
                site_occupancy[destination_site] = policy.compute_site_occupancy(site, self.inventory_manager)

                for replica in dataset.replicas:
                    site = replica.site
                    site_business[site] = policy.compute_site_business(site, self.inventory_manager, self.demand_manager)

                # check if we should stop copying
                if len(dataset.replicas) > dealer_config.max_replicas:
                    logger.warning('%s has reached the maximum number of replicas allowed.', dataset.name)
                    break
    
                if min(site_occupancy.values()) > config.target_site_occupancy:
                    logger.warning('All sites have exceeded target storage occupancy. No more copies will be made.')
                    global_stop = True
                    break
    
                if min(copy_volumes.values()) > dealer_config.max_copy_per_site:
                    logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                    global_stop = True
                    break

                if sum(copy_volumes.values()) > dealer_config.max_copy_total:
                    logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                    global_stop = True
                    break

            if global_stop:
                break

        return copy_list

    def commit_copies(self, run_number, copy_list, is_test):
        # first make sure the list of blocks is up-to-date
        datasets = []
        for site, replicas in copy_list.items():
            for replica in replicas:
                if replica.dataset not in datasets:
                    datasets.append(replica.dataset)

        self.inventory_manager.dataset_source.set_dataset_constituent_info(datasets)

        for site, replicas in copy_list.items():
            if len(replicas) == 0:
                continue

            copy_mapping = self.transaction_manager.copy.schedule_copies(replicas, comments = self.copy_message, is_test = is_test)
            # copy_mapping .. {operation_id: (approved, [replica])}
    
            for operation_id, (approved, replicas) in copy_mapping.items():
                if approved and not is_test:
                    self.inventory_manager.store.add_dataset_replicas(replicas)
                    self.inventory_manager.store.set_last_update()
    
                size = sum([r.size(physical = False) for r in replicas]) # this is not group size but the total size on disk

                self.history.make_copy_entry(run_number, site, operation_id, approved, [r.dataset for r in replicas], size, is_test = is_test)


if __name__ == '__main__':

    import sys
    import fnmatch
    import re
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from dealer.policy import Policy

    parser = ArgumentParser(description = 'Use dealer to copy a specific dataset from a specific site.')

    parser.add_argument('replica', metavar = 'SITE:DATASET', help = 'Replica to delete.')
    parser.add_argument('--group', '-g', metavar = 'GROUP', dest = 'group', default = 'AnalysisOps', help = 'Group name.')
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

    # create a subclass of Policy that allows direct manipulation of specific replicas.
    class DirectCopy(Policy):
        def __init__(self, group, quotas, partition, site_pattern, dataset_pattern):
            Policy.__init__(self, group, quotas, partition)

            self._site_re = re.compile(fnmatch.translate(site_pattern))
            self._dataset_re = re.compile(fnmatch.translate(dataset_pattern))

        def applies(self, replica): # override
            return self._site_re.match(replica.site.name) and self._dataset_re.match(replica.dataset.name)

        def need_copy(self, dataset, demand): # override
            return True


    group = inventory_manager.groups(args.group)
    sites = inventory_manager.sites.keys()
    quotas = {}
    for site in sites:
        quotas[site] = site.quota(group)

    direct_copy = DirectCopy(group, quotas, args.group, site_pattern, dataset_pattern)

    dealer.set_policy(policy, partition = args.group)

    if args.dry_run:
        config.read_only = True

    dealer.run(partition = args.partition, is_test = not args.production_run)
