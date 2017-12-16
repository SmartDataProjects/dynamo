import time
import datetime
import collections
import fnmatch
import logging
import random

from dataformat import Dataset, DatasetReplica, Block, BlockReplica, Site

LOG = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, config):
        """
        @param config      Configuration
        """
        
        self.copy_op = getattr(operation.impl, config.copy_op.module)(config.copy_op.config)
        self.history = getattr(history.impl, config.history.module)(config.history.config)

        self._setup_policy(config)

        # Do not copy data to sites beyond target occupancy fraction (0-1)
        self.target_site_occupancy = config.target_site_occupancy
        # At each site: schedule dataset copies until the volume exceeds max_copy_per_site.
        # The value is given in TB in the configuration file
        self.max_copy_per_site = config.max_copy_per_site * 1.e+12
        # Overall: schedule dataset copies until the volume exceeds max_copy_per_site.
        # The value is given in TB in the configuration file
        self.max_copy_total = config.max_copy_total * 1.e+12

    def run(self, inventory, comment = ''):
        """
        Main executable.
        1. Update site status.
        2. Take snapshots of the current status (datasets and sites).
        3. Collect copy requests from various plugins, sorted by priority.
        4. Go through the list of requests and fulfill up to the allowed volume.
        5. Make transfer requests.
        @param inventory  Dynamo inventory
        @param comment    Passed to dynamo history
        """

        LOG.info('Dealer run for %s starting at %s', policy.partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

        LOG.info('Updating dataset demands.')
        self.demand_manager.update(inventory, policy.used_demand_plugins)



        all_sites = self.inventory_manager.sites.values()

        quotas = dict((site, site.partition_quota(policy.partition)) for site in all_sites)

        # Ask each site if it should be considered as a copy destination.
        target_sites = set()
        for site in all_sites:
            if quotas[site] > 0. and \
                    site.status == Site.STAT_READY and \
                    policy.target_site_def(site) and \
                    site.storage_occupancy(policy.partition, physical = False) < self.target_site_occupancy:

                target_sites.add(site)

        if len(target_sites) == 0:
            LOG.info('No sites can accept transfers at the moment. Exiting Dealer.')
            return

        run_number = self.history.new_copy_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        # update site and dataset lists
        # take a snapshot of site status
        # take snapshots of quotas if updated
        self.history.save_sites(all_sites)
        self.history.save_datasets(self.inventory_manager.datasets.values())

        pending_volumes = collections.defaultdict(float)
        # TODO get input from transfer monitor and update the pending volumes

        LOG.info('Collecting copy proposals.')

        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too - but is not passed the list of target sites to keep things simpler
        requests = policy.collect_requests(self.inventory_manager)

        LOG.info('Determining the list of transfers to make.')

        copy_list = self.determine_copies(target_sites, requests, policy, pending_volumes)

        policy.record(run_number, self.history, copy_list)

        LOG.info('Committing copy.')

        comment = 'Dynamo -- Automatic replication request for %s partition.' % policy.partition.name
        self.commit_copies(run_number, copy_list, policy.group, is_test, comment)

        self.history.close_copy_run(run_number)

        LOG.info('Finished dealer run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

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
                make_new_replica = self._add_dataset_to_site

            elif type(item) is Block:
                item_name = item.dataset.name + '#' + item.real_name()
                item_size = item.size * 1.e-12
                find_replica_at = lambda s: s.find_block_replica(item)
                make_new_replica = self._add_block_to_site

            elif type(item) is list:
                # list of blocks (must belong to the same dataset)
                if len(item) == 0:
                    continue

                dataset = item[0].dataset
                item_name = dataset.name
                item_size = sum(b.size for b in item) * 1.e-12
                find_replica_at = lambda s: s.find_dataset_replica(dataset)
                make_new_replica = self._add_blocks_to_site

            else:
                LOG.warning('Invalid request found. Skipping.')
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
                    LOG.warning('%s has no copy destination.', item_name)
                    continue

                x = random.uniform(0., site_array[-1][1])
        
                isite = next(k for k in range(len(site_array)) if x < site_array[k][1])
        
                destination = site_array[isite][0]

            else:
                if destination not in site_occupancy or \
                        site_occupancy[destination] + item_size / quotas[destination] > 1. or \
                        not policy.is_allowed_destination(item, destination):
                    # a plugin specified the destination, but it's not in the list of potential target sites
                    LOG.warning('Cannot copy %s to %s.', item_name, destination.name)
                    continue

                if find_replica_at(destination) is not None:
                    LOG.info('%s is already at %s', item_name, destination.name)
                    continue

            LOG.info('Copying %s to %s', item_name, destination.name)

            new_replica = make_new_replica(item, destination, policy.group)

            copy_list[destination].append(new_replica)

            # recompute site properties
            pending_volumes[destination] += item_size
            site_occupancy[destination] += item_size / quotas[destination]

            if site_occupancy[destination] > self.target_site_occupancy or \
                    pending_volumes[destination] > self.max_copy_per_site:
                LOG.info('Site %s projected occupancy exceeded the limit.', destination.name)
                # this site should get no more copies
                site_occupancy.pop(destination)

            # check if we should stop copying
            if min(pending_volumes.itervalues()) > self.max_copy_per_site:
                LOG.warning('All sites have exceeded copy volume target. No more copies will be made.')
                break

            if sum(pending_volumes.itervalues()) > self.max_copy_total:
                LOG.warning('Total copy volume has exceeded the limit. No more copies will be made.')
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
                    # report back to main thread
                    pass
    
                size = sum([r.size(physical = False) for r in op_replicas]) # this is not group size but the total size on disk

                datasets = []
                for rep in op_replicas:
                    if type(rep) is DatasetReplica:
                        datasets.append(rep.dataset)
                    elif type(rep) is BlockReplica:
                        datasets.append(rep.block.dataset)

                self.history.make_copy_entry(run_number, site, operation_id, approved, [r.dataset for r in op_replicas], size)

    def _add_dataset_to_site(self, dataset, site, group):
        replica = DatasetReplica(dataset, site)
        self.inventory_manager.update(replica)
        for block in dataset.blocks:
            block_replica = BlockReplica(block, site, group, size = 0, last_update = 0)
            self.inventory_manager.update(block_replica)

        return replica

    def _add_block_to_site(self, block, site, group):
        block_replica = BlockReplica(block, site, group, size = 0, last_update = 0)
        self.inventory_manager.update(block_replica)

        return block_replica

    def _add_blocks_to_site(self, blocks, site, group):
        dataset = blocks[0].dataset
        replica = DatasetReplica(dataset, site)
        self.inventory_manager.update(replica)
        for block in blocks:
            block_replica = BlockReplica(block, site, group, size = 0, last_update = 0)
            self.inventory_manager.update(block_replica)
        
        return replica
