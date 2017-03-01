import time
import datetime
import collections
import fnmatch
import logging

from common.dataformat import Dataset, DatasetReplica, BlockReplica, Site
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

        try:
            self.demand_manager.update(self.inventory_manager, accesses = False, requests = True, locks = False)
            self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates
    
            policy = self.policies[partition]
    
            run_number = self.history.new_copy_run(partition.name, policy.version, is_test = is_test, comment = comment)
    
            # update site and dataset lists
            # take a snapshot of site status
            self.history.save_sites(run_number, self.inventory_manager)
            self.history.save_datasets(run_number, self.inventory_manager)
            # take snapshots of quotas if updated
            quotas = dict((site, site.partition_quota(partition)) for site in self.inventory_manager.sites.values())
            self.history.save_quotas(run_number, quotas)
   
            logger.info('Identifying target sites.')
    
            # Ask each site if it should be considered as a copy destination.
            target_sites = set()
            for site in self.inventory_manager.sites.values():
                if site.partition_quota(partition) != 0. and site.status == Site.STAT_READY and site.active == Site.ACT_AVAILABLE and policy.target_site_def.match(site):
                    target_sites.add(site)
    
            pending_volumes = collections.defaultdict(float)
            # TODO get input from transfer monitor and update the pending volumes
    
            # all datasets that the policy considers
            requests = policy.collect_requests(self.inventory_manager)

            items = self.prioritize(requests)
    
            copy_list = self.determine_copies(target_sites, items, policy, pending_volumes)

            policy.record(run_number, self.history, copy_list)
    
            logger.info('Committing copy.')
            self.commit_copies(run_number, policy, copy_list, is_test, comment, auto_approval)
    
            self.history.close_copy_run(run_number)

        finally:
            pass

        logger.info('Finished dealer run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def prioritize(self, requests):
        """
        For the moment do the dumbest thing
        """

        items = ([], [], [])
        for plugin, (datasets, blocks, files) in requests.items():
            items[0].extend(datasets)
            items[1].extend(blocks)
            items[2].extend(files)

        return items

    def determine_copies(self, sites, items, policy, pending_volumes):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.

        @param sites  List of site objects
        @param items  ([datasets], [blocks], [files]) where each list element can be the object or (object, destination_site)
        @param policy Dealer policy
        @param pending_volumes Volumes pending transfer
        """
        
        copy_list = dict([(site, []) for site in sites]) # site -> [new_replica]

        datasets, blocks, files = items

        def compute_site_business(site):
            business = 0.
    
            for replica in list(site.dataset_replicas):
                dataset = replica.dataset
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

        quota = {}
        for site in sites:
            quota[site] = site.partition_quota(policy.partition)

        # now go through datasets
        for entry in datasets:
            if type(entry) is tuple:
                dataset, destination_site = entry
            else:
                dataset = entry
                destination_site = None

            dataset_size = dataset.size() * 1.e-12

            if destination_site is None:
                sorted_sites = sorted(site_business.items(), key = lambda (s, n): n) #sorted from emptiest to busiest

                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        site_occupancy[dest] + dataset_size < quota[dest] and \
                        dest.find_dataset_replica(dataset) is None
                    )
    
                except StopIteration:
                    logger.warning('%s has no copy destination.', dataset.name)
                    break

            logger.info('Copying %s to %s', dataset.name, destination_site.name)

            new_replica = self.inventory_manager.add_dataset_to_site(dataset, destination_site, policy.group)

            copy_list[destination_site].append(new_replica)

            # recompute site properties
            pending_volumes[destination_site] += dataset_size
            occ = site.storage_occupancy(policy.partition, physical = False)

            if occ > dealer_config.target_site_occupancy * dealer_config.overflow_factor or \
                    pending_volumes[destination_site] < dealer_config.max_copy_per_site:
                # this site should get no more copies
                site_business.pop(destination_site)
                site_occupancy.pop(destination_site)
            else:
                site_occupancy[destination_site] = occ

            for replica in dataset.replicas:
                site = replica.site
                if site in site_business:
                    site_business[site] = compute_site_business(site)

            # check if we should stop copying
            if min(pending_volumes.values()) > dealer_config.max_copy_per_site:
                logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.values()) > dealer_config.max_copy_total:
                logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        # now go through blocks
        for entry in blocks:
            if type(entry) is tuple:
                block, destination_site = entry
            else:
                block = entry
                destination_site = None

            block_size = block.size * 1.e-12
            block_name = block.real_name()

            if destination_site is None:
                sorted_sites = sorted(site_business.items(), key = lambda (s, n): n) #sorted from emptiest to busiest

                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        site_occupancy[dest] + block_size < quota[dest] and \
                        dest.find_block_replica(block) is None
                    )
    
                except StopIteration:
                    logger.warning('%s#%s has no copy destination.', block.dataset.name, block_name)
                    break

            logger.info('Copying %s#%s to %s', block.dataset.name, block_name, destination_site.name)

            new_replica = self.inventory_manager.add_block_to_site(block, destination_site, policy.group)

            copy_list[destination_site].append(new_replica)

            # recompute site properties
            pending_volumes[destination_site] += block_size
            occ = site.storage_occupancy(policy.partition, physical = False)

            if occ > dealer_config.target_site_occupancy * dealer_config.overflow_factor or \
                    pending_volumes[destination_site] < dealer_config.max_copy_per_site:
                # this site should get no more copies
                site_business.pop(destination_site)
                site_occupancy.pop(destination_site)
            else:
                site_occupancy[destination_site] = occ

            for replica in block.dataset.replicas:
                site = replica.site
                if site in site_business:
                    site_business[site] = compute_site_business(site)

            # check if we should stop copying
            if min(pending_volumes.values()) > dealer_config.max_copy_per_site:
                logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.values()) > dealer_config.max_copy_total:
                logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        # now go through files
        for entry in files:
            if type(entry) is tuple:
                lfile, destination_site = entry
            else:
                lfile = entry
                destination_site = None

            file_size = lfile.size * 1.e-12

            if destination_site is None:
                sorted_sites = sorted(site_business.items(), key = lambda (s, n): n) #sorted from emptiest to busiest
    
                try:
                    destination_site = next(dest for dest, njob in sorted_sites if \
                        site_occupancy[dest] + file_size < quota[dest] and \
                        dest.find_dataset_replica(dataset) is None
                    )
    
                except StopIteration:
                    logger.warning('%s has no copy destination.', lfile.fullpath())
                    break

            logger.info('Copying %s to %s', lfile.fullpath(), destination_site.name)

            # there is no mechanism of tracking the physical movement of files in inventory.

            copy_list[destination_site].append((site, lfile))

            # recompute site properties
            pending_volumes[destination_site] += file_size
            occ = site.storage_occupancy(policy.partition, physical = False)

            if occ > dealer_config.target_site_occupancy * dealer_config.overflow_factor or \
                    pending_volumes[destination_site] < dealer_config.max_copy_per_site:
                # this site should get no more copies
                site_business.pop(destination_site)
                site_occupancy.pop(destination_site)
            else:
                site_occupancy[destination_site] = occ

            # check if we should stop copying
            if min(pending_volumes.values()) > dealer_config.max_copy_per_site:
                logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.values()) > dealer_config.max_copy_total:
                logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
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

            copy_mapping = self.transaction_manager.copy.schedule_copies(replicas, policy.group, comments = comment, auto_approval = auto_approval, is_test = is_test)
            # copy_mapping .. {operation_id: (approved, [replica])}
    
            for operation_id, (approved, op_replicas) in copy_mapping.items():
                if approved and not is_test:
                    self.inventory_manager.store.add_datasetreplicas(op_replicas)
    
                size = sum([r.size(physical = False) for r in op_replicas]) # this is not group size but the total size on disk

                datasets = []
                for rep in op_replicas:
                    if type(rep) is DatasetReplica:
                        datasets.append(rep.dataset)
                    elif type(rep) is BlockReplica:
                        datasets.append(rep.block.dataset)

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
