import time
import datetime
import collections
import fnmatch
import logging
import random

from dynamo.dataformat import Dataset, DatasetReplica, Block, BlockReplica, Site, ConfigurationError
from dynamo.dealer.dealerpolicy import DealerPolicy
from dynamo.operation.copy import CopyInterface
from dynamo.history.history import TransactionHistoryInterface
from dynamo.utils.signaling import SignalBlocker
import dynamo.dealer.plugins as dealer_plugins
import dynamo.policy.producers as producers

LOG = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, config):
        """
        @param config      Configuration
        """

        self.copy_op = CopyInterface.get_instance(config.copy_op.module, config.copy_op.config)

        if 'history' in config:
            self.history = TransactionHistoryInterface.get_instance(config.history.module, config.history.config)
        else:
            self.history = TransactionHistoryInterface.get_instance()

        if config.test_run:
            self.copy_op.dry_run = True
            self.history.test = True

        self._attr_producers = []

        self.policy = DealerPolicy(config)
        self._setup_plugins(config, config.test_run)

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
        cycle_number = self.history.new_copy_cycle(self.policy.partition_name, self.policy.version, comment = comment)

        LOG.info('Dealer cycle %d for %s starting', cycle_number, self.policy.partition_name)

        LOG.info('Identifying target sites.')
        partition = inventory.partitions[self.policy.partition_name]
        # Group of newly created replicas
        group = inventory.groups[self.policy.group_name]

        # Ask each site if it should be considered as a copy destination.
        self.policy.set_target_sites(inventory.sites.itervalues(), partition)

        if len(self.policy.target_sites) == 0:
            LOG.info('No sites can accept transfers at this moment. Exiting Dealer.')
            return

        LOG.info('Loading dataset attrs.')
        for plugin in self._attr_producers:
            plugin.load(inventory)

        LOG.info('Saving site and dataset names.')
        self.history.save_sites(inventory.sites.itervalues())
        self.history.save_datasets(inventory.datasets.itervalues())

        LOG.info('Collecting copy proposals.')
        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too, but are not passed the list of target sites
        # to keep things simpler. If a plugin proposes a copy to a non-target site, the proposal is
        # ignored.
        # requests is [(item, destination, plugin)]
        requests = self._collect_requests(inventory)

        LOG.info('Determining the list of transfers to make.')
        # copy_list is {plugin: [new dataset replica]}
        copy_list = self._determine_copies(partition, group, requests)

        LOG.info('Saving the record')
        for plugin, replicas in copy_list.iteritems():
            plugin.postprocess(cycle_number, self.history, copy_list[plugin])

        # We don't care about individual plugins any more - flatten the copy list
        # Resolve potential overlaps (one plugin can request a part of dataset requested by another)
        # by making everything into dataset replicas
        # Group is fixed for a single run() for now, so there is no problem of overwriting ownerships
        unique_map = {}
        for plugin, replicas in copy_list.iteritems():
            for replica in replicas:
                try:
                    unique_map[(replica.site, replica.dataset)].block_replicas.update(replica.block_replicas)
                except KeyError:
                    unique_map[(replica.site, replica.dataset)] = replica

        unique_replicas = unique_map.values()

        LOG.info('Committing copy.')
        comment = 'Dynamo -- Automatic replication request for %s partition.' % partition.name
        self._commit_copies(cycle_number, inventory, unique_replicas, comment)

        self.history.close_copy_cycle(cycle_number)

        LOG.info('Dealer cycle completed')

    def get_plugins(self):
        return self._plugin_priorities.keys()

    def _setup_plugins(self, config, is_test_run):
        self._plugin_priorities = {}

        n_zero_prio = 0
        n_nonzero_prio = 0
        for name, spec in config.plugins.items():
            plugin = getattr(dealer_plugins, spec.module)(spec.config)
            plugin.read_only = is_test_run
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

        # Set up dataset attribute providers
        attrs_config = config.attrs

        attr_names = set()
        for plugin in self._plugin_priorities.keys():
            attr_names.update(plugin.required_attrs)

        producer_names = set()
        for attr_name in attr_names:
            # Find the provider of each dataset attribute
            producer_cls = ''
            for cls in producers.producers[attr_name]:
                if cls in attrs_config:
                    if producer_cls:
                        LOG.error('Attribute %s is provided by two producers: [%s %s]', attr_name, producer_cls, cls)
                        LOG.error('Please fix the configuration so that each dataset attribute is provided by a unique producer.')
                        raise ConfigurationError('Duplicate attribute producer')

                    producer_cls = cls

            producer_names.add(producer_cls)

        for producer_cls in producer_names:
            self._attr_producers.append(getattr(producers, producer_cls)(attrs_config[producer_cls]))

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

            if len(reqlist) == 0:
                LOG.debug('No more requests from %s', plugin.name)
                reqlists.pop(plugin)

            item, destination = request

            if type(item) is Dataset:
                dataset = item
                # check that there is at least one source (allow it to be incomplete - could be in production)
                if len(dataset.replicas) == 0:
                    continue

                name = item.name
            elif type(item) is Block:
                if len(item.replicas) == 0:
                    continue

                dataset = item.dataset
                name = item.full_name()
            elif type(item) is list:
                no_source = False
                for block in item:
                    if len(block.replicas) == 0:
                        no_source = True
                        break

                if no_source:
                    continue

                dataset = item[0].dataset
                name = dataset.name + '#'
                name += ':'.join(block.real_name() for block in item)

            if dataset.status not in (Dataset.STAT_PRODUCTION, Dataset.STAT_VALID):
                continue

            if LOG.getEffectiveLevel() == logging.DEBUG:
                if destination is None:
                    destname = 'somewhere'
                else:
                    destname = destination.name
    
                LOG.debug('Selecting request from %s: %s to %s', plugin.name, name, destname)

            requests.append(request + (plugin,))

        return requests

    def _determine_copies(self, partition, group, requests):
        """
        @param partition       Partition we copy into.
        @param group           Make new replicas owned by this group.
        @param requests        [(item, destination, plugin)], where item is a Dataset, Block, or [Block]
        @return {plugin: [new dataset replica]}
        """

        # returned dict
        copy_list = collections.defaultdict(list)
        copy_volumes = dict((site, 0.) for site in self.policy.target_sites) # keep track of how much we are assigning to each site

        stats = {}
        for plugin in self._plugin_priorities.keys():
            stats[plugin.name] = {}

        reject_stats = {
            'Not a target site': 0,
            'Replica exists': 0,
            'Not allowed': 0,
            'Destination is full': 0,
            'Invalid request': 0,
            'No destination available': 0
        }

        # now go through all requests
        for item, destination, plugin in requests:
            if destination is None:
                # Randomly choose the destination site with probability proportional to free space
                destination, item_name, item_size, reject_reason = self.policy.find_destination_for(item, group, partition)
            else:
                # Check the destination availability
                item_name, item_size, reject_reason = self.policy.check_destination(item, destination, group, partition)

            if reject_reason is not None:
                reject_stats[reject_reason] += 1
                continue

            LOG.debug('Copying %s to %s requested by %s', item_name, destination.name, plugin.name)
            try:
                stat = stats[plugin.name][destination.name]
            except KeyError:
                stat = (0, 0)

            stats[plugin.name][destination.name] = (stat[0] + 1, stat[0] + item_size)

            if type(item) is Dataset:
                dataset = item
                blocks = dataset.blocks
            elif type(item) is Block:
                dataset = block.dataset
                blocks = [item]
            elif type(item) is list:
                blocks = item
                dataset = blocks[0].dataset

            new_replica = DatasetReplica(dataset, destination)
            for block in blocks:
                new_replica.block_replicas.add(BlockReplica(block, destination, group, size = 0))

            copy_list[plugin].append(new_replica)
            # New replicas may not be in the target partition, but we add the size up to be conservative
            copy_volumes[destination] += item_size

            if not self.policy.is_target_site(destination.partitions[partition], copy_volumes[destination]):
                LOG.info('%s is not a target site any more.', destination.name)
                self.policy.target_sites.remove(destination)

            if sum(copy_volumes.itervalues()) > self.policy.max_total_cycle_volume:
                LOG.warning('Total copy volume has exceeded the limit. No more copies will be made.')
                break

        for plugin_name in sorted(stats.keys()):
            plugin_stats = stats[plugin_name]
            for destination_name in sorted(plugin_stats.keys()):
                dest_stats = plugin_stats[destination_name]
                LOG.info('Plugin %s requests %d items (%.1f TB) to %s', plugin_name, dest_stats[0], dest_stats[1] * 1.e-12, destination_name)

        for reason in sorted(reject_stats.keys()):
            LOG.info('%d items rejected for [%s]', reject_stats[reason], reason)

        return copy_list

    def _commit_copies(self, cycle_number, inventory, copy_list, comment):
        """
        @param cycle_number  Cycle number.
        @param inventory     Dynamo inventory.
        @param copy_list     List of dataset replicas.
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
                # means we can let each plugin decide which group they want to make replicas in
                for copy_id, (approved, site, items) in copy_mapping.iteritems():
                    dataset_sizes = collections.defaultdict(int)
                    # items: mixed list of datasets and blocks
                    for item in items:
                        if type(item) is Dataset:
                            dataset_sizes[item] = item.size
                            if approved:
                                inventory.update(DatasetReplica(item, site))
                                for block in item.blocks:
                                    inventory.update(BlockReplica(block, site, group, size = 0))

                        else:
                            dataset_sizes[item.dataset] += item.size
                            if approved:
                                if site.find_dataset_replica(item.dataset) is None:
                                    inventory.update(DatasetReplica(item.dataset, site))

                                inventory.update(BlockReplica(item, site, group, size = 0))
    
                    self.history.make_copy_entry(cycle_number, site, copy_id, approved, dataset_sizes.items())
