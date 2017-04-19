import time
import datetime
import collections
import fnmatch
import logging

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

    def run(self, policy, is_test = False, comment = '', auto_approval = True):
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

        self.demand_manager.update(self.inventory_manager, accesses = False, requests = True, locks = False)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        run_number = self.history.new_copy_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, self.inventory_manager)
        self.history.save_datasets(run_number, self.inventory_manager)
        # take snapshots of quotas if updated
        quotas = dict((site, site.partition_quota(policy.partition)) for site in self.inventory_manager.sites.values())
        self.history.save_quotas(run_number, quotas)

        pending_volumes = collections.defaultdict(float)
        # TODO get input from transfer monitor and update the pending volumes

        logger.info('Collecting copy proposals.')

        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too - but is not passed the list of target sites to keep things simpler
        requests = policy.collect_requests(self.inventory_manager)

        logger.info('Determining the list of transfers to make.')

        # Ask each site if it should be considered as a copy destination.
        target_sites = set()
        for site in self.inventory_manager.sites.values():
            if quotas[site] != 0. and \
                    site.status == Site.STAT_READY and \
                    site.active == Site.ACT_AVAILABLE and \
                    policy.target_site_def(site) and \
                    site.storage_occupancy(policy.partition, physical = False) < dealer_config.target_site_occupancy:

                target_sites.add(site)

        copy_list = self.determine_copies(target_sites, requests, policy.partition, policy.group, pending_volumes)

        policy.record(run_number, self.history, copy_list)

        logger.info('Committing copy.')

        self.commit_copies(run_number, copy_list, policy.group, is_test, comment, auto_approval)

        self.history.close_copy_run(run_number)

        logger.info('Finished dealer run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_copies(self, target_sites, requests, partition, group, pending_volumes):
        """
        Algorithm:
        1. Compute a time-weighted sum of number of requests for the last three days.
        2. Decide the sites least-occupied by analysis activities.
        3. Copy datasets with number of requests > available replicas to empty sites.

        @param sites  List of target sites
        @param items  ([datasets], [blocks], [files]) where each list element can be the object or (object, destination_site)
        @param policy Dealer policy
        @param pending_volumes Volumes pending transfer
        """

        quotas = dict((site, site.partition_quota(partition)) for site in self.inventory_manager.sites.values())

        copy_list = dict([(site, []) for site in target_sites]) # site -> [new_replica]

        site_occupancy = {}
        for site in target_sites:
            # At the moment we don't have the information of exactly how many jobs are running at each site, so we are simply sorting the sites by occupancy.
            site_occupancy[site] = site.storage_occupancy(partition, physical = False)

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
                find_replica_at = lambda s: s.find_dataset_replica(item)
                make_new_replica_at = lambda s: self.inventory_manager.add_dataset_to_site(item, s, group)

            elif type(item) is Block:
                item_name = item.dataset.name + '#' + item.real_name()
                find_replica_at = lambda s: s.find_block_replica(item)
                make_new_replica_at = lambda s: self.inventory_manager.add_block_to_site(item, s, group)

            elif type(item) is list:
                # list of blocks (must belong to the same dataset)
                if len(item) == 0:
                    continue

                dataset = item[0].dataset
                item_name = dataset.name
                find_replica_at = lambda s: s.find_dataset_replica(dataset)
                make_new_replica_at = lambda s: self.inventory_manager.add_dataset_to_site(dataset, s, group, blocks = items)

            else:
                logger.warning('Invalid request found. Skipping.')
                continue

            size = item.size * 1.e-12

            if destination is None:
                #sorted from emptiest to busiest
                sorted_sites = sorted(site_occupancy.items(), key = lambda (s, f): f)
                
                logger.info('Sites sorted by occupancy: %s', str(['%s(%.2f)' % (s.name, f) for s, f in sorted_sites]))

                try:
                    destination = next(site for site, occupancy in sorted_sites if \
                        occupancy + size / quotas[site] < 1. and \
                        find_replica_at(site) is None
                    )
    
                except StopIteration:
                    logger.warning('%s has no copy destination.', item_name)
                    continue

            else:
                if destination not in site_occupancy or site_occupancy[site] + size / quotas[site] > 1.:
                    # a plugin specified the destination, but it's not in the list of potential target sites
                    logger.warning('Cannot copy %s to %s.', item_name, site.name)
                    continue

                if find_replica_at(destination) is not None:
                    logger.info('%s is already at %s', item_name, destination.name)
                    continue

            logger.info('Copying %s to %s', item_name, destination.name)

            new_replica = make_new_replica_at(destination)

            copy_list[destination].append(new_replica)

            # recompute site properties
            pending_volumes[destination] += size
            site_occupancy[destination] += size / quotas[destination]

            if site_occupancy[destination] > dealer_config.target_site_occupancy or \
                    pending_volumes[destination] > dealer_config.max_copy_per_site:
                # this site should get no more copies
                site_occupancy.pop(destination)

            # check if we should stop copying
            if min(pending_volumes.values()) > dealer_config.max_copy_per_site:
                logger.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.values()) > dealer_config.max_copy_total:
                logger.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        return copy_list

    def commit_copies(self, run_number, copy_list, group, is_test, comment, auto_approval):
        for site, replicas in copy_list.items():
            if len(replicas) == 0:
                continue

            for replica in list(replicas):
                # final check with replica information source
                if self.inventory_manager.replica_source.replica_exists_at_site(site, replica):
                    logger.info('Not copying replica because it exists at site: %s', repr(replica))
                    replicas.remove(replica)

            copy_mapping = self.transaction_manager.copy.schedule_copies(replicas, group, comments = comment, auto_approval = auto_approval, is_test = is_test)
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
