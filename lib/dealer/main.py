import time
import datetime
import collections
import fnmatch
import logging
import random

from dataformat import Dataset, DatasetReplica, Block, BlockReplica, Site
from dealer.dealerpolicy import DealerPolicy
import dealer.plugins
from demand.demand import DemandManager
import operation.impl
import history.impl
from utils.signal import SignalBlocker

LOG = logging.getLogger(__name__)

## Subroutines for creating replicas out of an object and a site

def new_replica_from_dataset(dataset, site, group):
    replica = DatasetReplica(dataset, site)
    for block in dataset.blocks:
        replica.block_replicas.add(BlockReplica(block, site, group, size = 0))

    return replica

def new_replica_from_block(block, site, group):
    return BlockReplica(block, site, group, size = 0)

def new_replica_from_blocks(blocks, site, group):
    dataset = blocks[0].dataset
    replica = DatasetReplica(dataset, site)
    for block in blocks:
        replica.block_replicas.add(BlockReplica(block, site, group, size = 0))

    return replica


class Dealer(object):

    def __init__(self, config):
        """
        @param config      Configuration
        """
        
        self.copy_op = getattr(operation.impl, config.copy_op.module)(config.copy_op.config)
        self.history = getattr(history.impl, config.history.module)(config.history.config)

        self.demand_manager = DemandManager(config.demand)
        self._used_demand_plugins = set()

        self.policy = DealerPolicy(config)
        self._setup_plugins(config)

    def run(self, inventory, comment = ''):
        """
        Main executable.
        2. Take snapshots of the current status (datasets and sites).
        3. Collect copy requests from various plugins, sorted by priority.
        4. Go through the list of requests and fulfill up to the allowed volume.
        5. Make transfer requests.
        @param inventory  Dynamo inventory
        @param comment    Passed to dynamo history
        """

        # fetch the deletion cycle number
        cycle_number = self.history.new_copy_run(self.policy.partition_name, self.policy.version, comment = comment)

        LOG.info('Dealer cycle %d for %s starting', cycle_number, self.policy.partition_name)

        LOG.info('Identifying target sites.')
        partition = inventory.partitions[self.policy.partition_name]
        # Group of newly created replicas
        group = inventory.groups[self.policy.group_name]
        quotas = dict((s, s.partitions[partition].quota) for s in inventory.sites.itervalues())

        # Ask each site if it should be considered as a copy destination.
        target_sites = set()
        for site in quotas.keys():
            if self._is_target_site(site.partitions[partition]):
                target_sites.add(site)

        if len(target_sites) == 0:
            LOG.info('No sites can accept transfers at this moment. Exiting Dealer.')
            return

        LOG.info('Updating dataset demands.')
#        self.demand_manager.update(inventory, self._used_demand_plugins)

        LOG.info('Saving site and dataset names.')
        self.history.save_sites(quotas.keys())
        self.history.save_datasets(inventory.datasets.values())

        LOG.info('Collecting copy proposals.')
        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too, but are not passed the list of target sites
        # to keep things simpler. If a plugin proposes a copy to a non-target site, the proposal is
        # ignored.
        # requests is [(item, destination, plugin)]
        requests = self._collect_requests(inventory)

        LOG.info('Determining the list of transfers to make.')
        # copy_list is {plugin: [new_replica]}
        copy_list = self._determine_copies(target_sites, partition, group, requests)

        LOG.info('Saving the record')
        for plugin in self._plugin_priorities.keys():
            if plugin in copy_list:
                plugin.save_record(cycle_number, self.history, copy_list[plugin])

        # We don't care about individual plugins any more - flatten the copy list
        all_copies = sum(copy_list.itervalues(), [])

        LOG.info('Committing copy.')
        comment = 'Dynamo -- Automatic replication request for %s partition.' % partition.name
        self._commit_copies(cycle_number, inventory, all_copies, comment)

        self.history.close_copy_run(cycle_number)

        LOG.info('Dealer cycle completed')

    def _setup_plugins(self, config):
        self._plugin_priorities = {}

        n_zero_prio = 0
        n_nonzero_prio = 0
        for name, spec in config.plugins.items():
            plugin = getattr(dealer.plugins, spec.module)(spec.config)
            self._plugin_priorities[plugin] = spec.priority

            if spec.priority:
                n_zero_prio += 1
            else:
                n_nonzero_prio += 1

        if n_zero_prio != 0 and n_nonzero_prio != 0:
            LOG.warning('Throwing away finite-priority plugins to make away for zero-priority plugins.')
            for plugin, prio in self._plugin_priorities.items():
                if prio != 0:
                    self._plugin_priorities.pop(plugin)

        for plugin in self._plugin_priorities.keys():
            self._used_demand_plugins.update(plugin.used_demand_plugins)

    def _collect_requests(self, inventory):
        """
        Collect requests from each plugin and return a prioritized list.
        @param inventory    DynamoInventory instance.
        @return A list of (item, destination, plugin)
        """

        reqlists = {} # {plugin: reqlist} reqlist is [(item, destination)]

        for plugin, priority in self._plugin_priorities.items():
            if priority == 0:
                # all plugins must have priority 0 (see _setup_plugins)
                # -> treat all as equal.
                priority = 1

            plugin_requests = plugin.get_requests(inventory, self.history, self.policy)

            LOG.debug('%s requesting %d items', plugin.name, len(plugin_requests))

            if len(plugin_requests) != 0:
                reqlist = reqlists[plugin] = []

                for request in plugin_requests:
                    if type(request) is not tuple:
                        # Plugins can just request an item to be copied somewhere.
                        # Convert the request into a tuple
                        reqlist.append((request, None))
                    else:
                        reqlist.append(request)

        # Flattened list of requests [(item, destination, plugin)]
        requests = []

        while len(reqlists) != 0:
            # Classic weighted random-picking algorithm
            plugins = reqlists.keys()

            pvalues = [1. / self._plugin_priorities[p] for p in plugins]
            sums = [sum(pvalues[:i + 1]) for i in range(len(pvalues))]

            # Select k if sum(w_{i})_{i <= k-1} w_{k} < x < sum(w_{i})_{i <= k} for x in Uniform(0, sum(w_{i}))
            x = random.uniform(0., sums[-1])

            # Index of the selected plugin
            ip = next(k for k in range(len(sums)) if x < sums[k])
            plugin = plugins[ip]

            reqlist = reqlists[plugin]
            request = reqlist.pop(0)

            requests.append(request + (plugin,))

            if LOG.getEffectiveLevel() == logging.DEBUG:
                item, destination = request
   
                if type(item).__name__ == 'Dataset':
                    name = item.name
                elif type(item).__name__ == 'Block':
                    name = item.dataset.name + '#' + item.real_name()
                elif type(item) is list:
                    name = item[0].dataset.name + '#'
                    name += ':'.join(block.real_name() for block in item)

                if destination is None:
                    destname = 'somewhere'
                else:
                    destname = destination.name
    
                LOG.debug('Selecting request from %s: %s to %s', plugin.name, name, destname)

            if len(reqlist) == 0:
                LOG.debug('No more requests from %s', plugin.name)
                reqlists.pop(plugin)

        return requests

    def _determine_copies(self, target_sites, partition, group, requests):
        """
        @param target_sites    List of target sites
        @param partition       Partition we copy into.
        @param group           Make new replicas owned by this group.
        @param requests        [(item, destination, plugin)], where item is a Dataset, Block, or [Block]
        @return {plugin: [new_replica]}
        """

        copy_list = {}
        copy_volumes = dict((site, 0.) for site in target_sites) # keep track of how much we are assigning to each site

        # now go through all requests
        for item, destination, plugin in requests:
            if type(item) is Dataset:
                item_name = item.name
                item_size = item.size
                find_replica_at = lambda s: s.find_dataset_replica(item)
                make_new_replica = new_replica_from_dataset

            elif type(item) is Block:
                item_name = item.dataset.name + '#' + item.real_name()
                item_size = item.size
                find_replica_at = lambda s: s.find_block_replica(item)
                make_new_replica = new_replica_from_block

            elif type(item) is list:
                # list of blocks (must belong to the same dataset)
                if len(item) == 0:
                    continue

                dataset = item[0].dataset
                item_name = dataset.name
                item_size = sum(b.size for b in item)
                find_replica_at = lambda s: s.find_dataset_replica(dataset)
                make_new_replica = new_replica_from_blocks

            else:
                LOG.warning('Invalid request found. Skipping.')
                continue

            if destination is None:
                # Randomly choose the destination site with probability proportional to free space

                site_array = []
                for site in target_sites:
                    site_partition = site.partitions[partition]

                    projected_occupancy = site_partition.occupancy_fraction(physical = False)
                    projected_occupancy += item_size / site_partition.quota

                    # total projected volume must not exceed the quota
                    if projected_occupancy > 1.:
                        continue

                    # replica must not be at the site already
                    if find_replica_at(site) is not None:
                        continue

                    # placement must be allowed by the policy
                    if not self.policy.is_allowed_destination(item, site):
                        continue

                    p = 1. - projected_occupancy
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
                # Check the destination availability

                if destination not in target_sites:
                    LOG.warning('Destination %s for %s is not a target site.', destination.name, item_name)

                if find_replica_at(destination) is not None:
                    LOG.info('%s is already at %s', item_name, destination.name)
                    continue
 
                site_partition = destination.partitions[partition]
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += item_size / site_partition.quota

                if occupancy_fraction > 1. or not self.policy.is_allowed_destination(item, destination):
                    # a plugin specified the destination, but it cannot be copied there
                    LOG.warning('Cannot copy %s to %s.', item_name, destination.name)
                    continue

            LOG.info('Copying %s to %s requested by %s', item_name, destination.name, plugin.name)

            new_replica = make_new_replica(item, destination, group)

            try:
                plugin_copy_list = copy_list[plugin]
            except KeyError:
                plugin_copy_list = copy_list[plugin] = []

            plugin_copy_list.append(new_replica)
            # New replicas may not be in the target partition, but we add the size up to be conservative
            copy_volumes[destination] += item_size

            if not self._is_target_site(destination.partitions[partition], copy_volumes[destination]):
                LOG.info('%s is not a target site any more.', destination.name)
                target_sites.remove(destination)

            if sum(copy_volumes.itervalues()) > self.policy.max_total_cycle_volume:
                LOG.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        return copy_list

    def _commit_copies(self, cycle_number, inventory, copy_list, comment):
        """
        @param cycle_number  Cycle number.
        @param inventory     Dynamo inventory.
        @param copy_list     Flat list of dataset or block replicas.
        @param comment       Comment to be passed to the copy interface.
        """

        signal_blocker = SignalBlocker(logger = LOG)

        group = inventory.groups[self.policy.group_name]

        by_site = collections.defaultdict(list)
        for replica in copy_list:
            by_site[replica.site].append(replica)

        for site in sorted(by_site.iterkeys(), key = lambda s: s.name):
            replicas = by_site[site]

            LOG.info('Scheduling copy of %d replicas to %s.', len(replicas), site.name)

            with signal_blocker:
                copy_mapping = self.copy_op.schedule_copies(replicas, comments = comment)
        
                # It would be better if mapping from replicas to items is somehow kept
                # Then we can get rid of creating yet another replica object below, which
                # means we can let each plugin to decide which group they want to make replicas in
                for copy_id, (approved, site, items) in copy_mapping.iteritems():
                    dataset_list = set()
                    for item in items:
                        size += item.size

                        if type(item) is Dataset:
                            dataset_list.add(item)
                            if approved:
                                replica = new_replica_from_dataset(item, site, group)
                                inventory.update(replica)

                        else:
                            dataset_list.add(item.dataset)
                            if approved:
                                if site.find_dataset_replica(item.dataset) is None:
                                    replica = new_replica_from_dataset(item.dataset, site, group)
                                    inventory.update(replica)

                                replica = new_replica_from_block(item, site, group)
                                inventory.update(replica)
    
                    self.history.make_copy_entry(cycle_number, site, copy_id, approved, dataset_list, size)

    def _is_target_site(self, site_partition, additional_volume = 0.):
        if site_partition.quota <= 0.:
            LOG.debug('%s has quota %f TB <= 0', site_partition.site.name, site_partition.quota * 1.e-12)
            return False

        site = site_partition.site

        if site.status != Site.STAT_READY:
            LOG.debug('%s is not ready', site_partition.site.name)
            return False

        if not self.policy.target_site_def(site):
            LOG.debug('%s does not match target site def', site_partition.site.name)
            return False

        occupancy_fraction = site_partition.occupancy_fraction(physical = False)
        occupancy_fraction += additional_volume / site_partition.quota

        if occupancy_fraction > self.policy.target_site_occupancy:
            LOG.debug('%s occupancy fraction %f > %f', site_partition.site.name, occupancy_fraction, self.policy.target_site_occupancy)
            return False

        # Difference between projected and physical volumes
        pending_volume = occupancy_fraction * site_partition.quota
        pending_volume -= site_partition.occupancy_fraction(physical = True) * site_partition.quota

        if pending_volume > self.policy.max_site_pending_volume:
            LOG.debug('%s pending volume %f > %f', site_partition.site.name, pending_volume, self.policy.max_site_pending_volume)
            return False

        return True
