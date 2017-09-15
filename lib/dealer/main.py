import time
import datetime
import collections
import fnmatch
import logging
import random

from common.dataformat import Dataset, DatasetReplica, Block, BlockReplica, Site
import common.configuration as config
import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand, history):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

    def run(self, policy, is_test = False, comment = ''):
        """
        1. Update site status.
        2. Take snapshots of the current status (datasets and sites).
        3. Collect copy requests from various plugins, sorted by priority.
        4. Go through the list of requests and fulfill up to the allowed volume.
        5. Make transfer requests.
        """

        logger.info('Dealer run for %s starting at %s', policy.partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

        if not comment:
            comment = 'Dynamo -- Automatic replication request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        self.demand_manager.update(self.inventory_manager, policy.used_demand_plugins)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        all_sites = self.inventory_manager.sites.values()

        quotas = dict((site, site.partition_quota(policy.partition)) for site in all_sites)

        # Ask each site if it should be considered as a copy destination.
        target_sites = set()
        for site in all_sites:
            if quotas[site] > 0. and \
                    site.status == Site.STAT_READY and \
                    policy.target_site_def(site) and \
                    site.storage_occupancy(policy.partition, physical = False) < dealer_config.main.target_site_occupancy:

                target_sites.add(site)

        if len(target_sites) == 0:
            logger.info('No sites can accept transfers at the moment. Exiting Dealer.')
            return

        run_number = self.history.new_copy_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        # update site and dataset lists
        # take a snapshot of site status
        # take snapshots of quotas if updated
        self.history.save_sites(all_sites)
        self.history.save_datasets(self.inventory_manager.datasets.values())

        pending_volumes = collections.defaultdict(float)
        # TODO get input from transfer monitor and update the pending volumes

        logger.info('Collecting copy proposals.')

        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too - but is not passed the list of target sites to keep things simpler
        requests = policy.collect_requests(self.inventory_manager)

        logger.info('Determining the list of transfers to make.')

        copy_list = self.determine_copies(target_sites, requests, policy, pending_volumes)

        policy.record(run_number, self.history, copy_list)

        logger.info('Committing copy.')

        self.commit_copies(run_number, copy_list, policy.group, is_test, comment)

        self.history.close_copy_run(run_number)

        logger.info('Finished dealer run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_copies(self, target_sites, requests, policy, pending_volumes):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.

        @param target_sites    List of target sites
        @param requests        [(item, destination) or item], where item is a Dataset, Block, or [Block]
        @param policy          Dealer policy
        @param pending_volumes Volumes pending transfer, to be updated
        """

        quotas = dict((site, site.partition_quota(policy.partition)) for site in self.inventory_manager.sites.itervalues())
        copy_list = dict([(site, []) for site in target_sites]) # site -> [new_replica]

        site_occupancy = {}
        for site in target_sites:
            # At the moment we don't have the information of exactly how many jobs are running at each site, so we are simply sorting the sites by occupancy.
            site_occupancy[site] = site.storage_occupancy(policy.partition, physical = False)

        candidates = []
        for request in requests:
            if type(request) is tuple:
                candidates.append(request)
            else:
                candidates.append((request, None))

        # now go through all candidates
        for item, destination in candidates:
            if type(item) is Dataset:
                item_name = item.name
                item_size = item.size * 1.e-12
                find_replica_at = lambda s: s.find_dataset_replica(item)
                make_new_replica_at = lambda s: self.inventory_manager.add_dataset_to_site(item, s, policy.group)

            elif type(item) is Block:
                item_name = item.dataset.name + '#' + item.real_name()
                item_size = item.size * 1.e-12
                find_replica_at = lambda s: s.find_block_replica(item)
                make_new_replica_at = lambda s: self.inventory_manager.add_block_to_site(item, s, policy.group)

            elif type(item) is list:
                # list of blocks (must belong to the same dataset)
                if len(item) == 0:
                    continue

                dataset = item[0].dataset
                item_name = dataset.name
                item_size = sum(b.size for b in item) * 1.e-12
                find_replica_at = lambda s: s.find_dataset_replica(dataset)
                make_new_replica_at = lambda s: self.inventory_manager.add_dataset_to_site(dataset, s, policy.group, blocks = item)

            else:
                logger.warning('Invalid request found. Skipping.')
                continue

            if destination is None:
                # randomly choose the destination site with probability proportional to free space
                site_array = []
                for site, occupancy in site_occupancy.iteritems():
                    if occupancy + item_size / quotas[site] > 1. or find_replica_at(site) is not None:
                        continue

                    if not policy.is_allowed_destination(item, site):
                        continue

                    p = 1. - occupancy
                    if len(site_array) != 0:
                        p += site_array[-1][1]
    
                    site_array.append((site, p))

                if len(site_array) == 0:
                    logger.warning('%s has no copy destination.', item_name)
                    continue

                x = random.uniform(0., site_array[-1][1])
        
                isite = next(k for k in range(len(site_array)) if x < site_array[k][1])
        
                destination = site_array[isite][0]

            else:
                if destination not in site_occupancy or \
                        site_occupancy[destination] + item_size / quotas[destination] > 1. or \
                        not policy.is_allowed_destination(item, destination):
                    # a plugin specified the destination, but it's not in the list of potential target sites
                    logger.warning('Cannot copy %s to %s.', item_name, destination.name)
                    continue

                if dealer_config.main.skip_existing and find_replica_at(destination) is not None:
                    logger.info('%s is already at %s', item_name, destination.name)
                    continue

            logger.info('Copying %s to %s', item_name, destination.name)

            new_replica = make_new_replica_at(destination)

            copy_list[destination].append(new_replica)

            # recompute site properties
            pending_volumes[destination] += item_size
            site_occupancy[destination] += item_size / quotas[destination]

            if site_occupancy[destination] > dealer_config.main.target_site_occupancy or \
                    pending_volumes[destination] > dealer_config.main.max_copy_per_site:
                logger.info('Site %s projected occupancy exceeded the limit.', destination.name)
                # this site should get no more copies
                site_occupancy.pop(destination)

            # check if we should stop copying
            if min(pending_volumes.itervalues()) > dealer_config.main.max_copy_per_site:
                logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.itervalues()) > dealer_config.main.max_copy_total:
                logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        return copy_list

    def commit_copies(self, run_number, copy_list, group, is_test, comment):
        for site, replicas in copy_list.iteritems():
            if len(replicas) == 0:
                continue

            copy_mapping = self.transaction_manager.copy.schedule_copies(replicas, group, comments = comment, is_test = is_test)
            # copy_mapping .. {operation_id: (approved, [replica])}
    
            for operation_id, (approved, op_replicas) in copy_mapping.iteritems():
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
