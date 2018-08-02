import time
import datetime
import collections
import fnmatch
import logging
import random

from dynamo.dataformat import Dataset, DatasetReplica, BlockReplica
from dynamo.dataformat.history import CopiedReplica, HistoryRecord
from dynamo.dealer.dealerpolicy import DealerPolicy
from dynamo.dealer.history import DealerHistory
from dynamo.operation.copy import CopyInterface
from dynamo.utils.signaling import SignalBlocker
import dynamo.dealer.plugins as dealer_plugins
from dynamo.policy.producers import get_producers

LOG = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, config):
        """
        @param config      Configuration
        """

        if 'copy_op' in config:
            self.copy_op = CopyInterface.get_instance(config.copy_op.module, config.copy_op.config)
        else: # default setting
            self.copy_op = CopyInterface.get_instance()

        self.history = DealerHistory(config.get('history', None))

        self._attr_producers = []

        self.policy = DealerPolicy(config)

        self.test_run = config.get('test_run', False)
        if self.test_run:
            self.copy_op.set_read_only()

        self._setup_plugins(config)

    def set_read_only(self, value = True):
        self.copy_op.set_read_only(value)
        self.history.set_read_only(value)
        for plugin in self._plugin_priorities.keys():
            plugin.set_read_only(value)

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
        cycle_number = self.history.new_cycle(self.policy.partition_name, comment = comment, test = self.test_run)

        LOG.info('Dealer cycle %d for %s starting', cycle_number, self.policy.partition_name)

        LOG.info('Identifying target sites.')
        partition = inventory.partitions[self.policy.partition_name]

        # Ask each site if it should be considered as a copy destination.
        self.policy.set_target_sites(inventory.sites.itervalues(), partition)

        if len(self.policy.target_sites) == 0:
            LOG.info('No sites can accept transfers at this moment. Exiting Dealer.')
            return

        LOG.info('Loading dataset attrs.')
        for plugin in self._attr_producers:
            plugin.load(inventory)

        LOG.info('Collecting copy proposals.')
        # Prioritized lists of datasets, blocks, and files
        # Plugins can specify the destination sites too, but are not passed the list of target sites
        # to keep things simpler. If a plugin proposes a copy to a non-target site, the proposal is
        # ignored.
        # requests is [(DealerRequest, plugin)]
        requests = self._collect_requests(inventory)

        LOG.info('Determining the list of transfers to make.')
        # copy_list is {plugin: [new dataset replica]}
        copy_list = self._determine_copies(partition, requests)

        LOG.info('Saving the record')
        for plugin, replicas in copy_list.iteritems():
            plugin.postprocess(cycle_number, replicas)

        # We don't care about individual plugins any more - flatten the copy list
        # Resolve potential overlaps (one plugin can request a part of dataset requested by another)
        # If two plugins request the same block with different ownership, one with the higher priority (lower priority number) wins
        unique_map = {} # {(site, dataset): dataset_replica}
        for plugin in sorted(copy_list.iterkeys(), key = lambda p: self._plugin_priorities[p]):
            replicas = copy_list[plugin]

            for replica in replicas:
                key = (replica.site, replica.dataset)
                if key in unique_map:
                    reserved_replica = unique_map[key]
                    for block_replica in replica.block_replicas:
                        if reserved_replica.find_block_replica(block_replica.block) is None:
                            # this BR was not requested by the other (higher-priority) plugin
                            reserved_replica.block_replicas.add(block_replica)

                else:
                    unique_map[key] = replica

        flattened_replicas = unique_map.values()

        LOG.info('Committing copy.')
        comment = 'Dynamo -- Automatic replication request for %s partition.' % partition.name
        self._commit_copies(cycle_number, inventory, flattened_replicas, comment)

        self.history.close_cycle(cycle_number)

        LOG.info('Dealer cycle completed')

    def get_plugins(self):
        return self._plugin_priorities.keys()

    def _setup_plugins(self, config):
        self._plugin_priorities = {}

        n_zero_prio = 0
        n_nonzero_prio = 0
        for name, spec in config.plugins.items():
            modname, _, clsname = spec.module.partition(':')
            cls = getattr(__import__('dynamo.dealer.plugins.' + modname, globals(), locals(), [clsname]), clsname)
            plugin = cls(spec.config)
            if self.test_run:
                plugin.set_read_only()
            self._plugin_priorities[plugin] = spec.priority

            if spec.priority:
                n_zero_prio += 1
            else:
                n_nonzero_prio += 1

        if n_zero_prio != 0 and n_nonzero_prio != 0:
            LOG.warning('Throwing away finite-priority plugins to make way for zero-priority plugins.')
            for plugin, prio in self._plugin_priorities.items():
                if prio != 0:
                    self._plugin_priorities.pop(plugin)

        # Set up dataset attribute providers

        attr_names = set()
        for plugin in self._plugin_priorities.keys():
            attr_names.update(plugin.required_attrs)

        self.attr_producers = list(set(get_producers(attr_names, config.attrs).itervalues()))

    def _collect_requests(self, inventory):
        """
        Collect requests from each plugin and return a prioritized list.
        @param inventory    DynamoInventory instance.
        @return A list of (item, destination, plugin)
        """

        # Default group for newly created replicas
        default_group = inventory.groups[self.policy.group_name]

        reqlists = {} # {plugin: reqlist} reqlist is [(item, destination)]

        for plugin, priority in self._plugin_priorities.items():
            if priority == 0:
                # all plugins must have priority 0 (see _setup_plugins)
                # -> treat all as equal.
                priority = 1

            plugin_requests = plugin.get_requests(inventory, self.policy)

            LOG.debug('%s requesting %d items', plugin.name, len(plugin_requests))

            if len(plugin_requests) != 0:
                reqlists[plugin] = plugin_requests

        # Flattened list of (DealerRequest, plugin)
        requests = []

        # Collect the requests based on plugin priority
        reject_stats = {
            'No source replica available': 0,
            'Dataset is not valid': 0
        }

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

            # check that there is at least one source (allow it to be incomplete - could be in production)
            no_source = False
            if request.block is not None:
                if len(request.block.replicas) == 0:
                    no_source = True

            elif request.blocks is not None:
                if len(request.blocks) == 0:
                    no_source = True
                else:
                    # all blocks must have at least one copy
                    for block in request.blocks:
                        if len(block.replicas) == 0:
                            no_source = True
                            break

            elif request.dataset is not None:
                if len(request.dataset.replicas) == 0:
                    no_source = True

            if no_source:
                LOG.debug('%s has no source', request.item_name())
                reject_stats['No source replica available'] += 1
                continue

            if request.dataset.status not in (Dataset.STAT_PRODUCTION, Dataset.STAT_VALID):
                LOG.debug('Dataset of %s is not valid', request.item_name())
                reject_stats['Dataset is not valid'] += 1
                continue

            # set the group here
            if request.group is None:
                request.group = default_group

            if LOG.getEffectiveLevel() == logging.DEBUG:
                if request.destination is None:
                    destname = 'somewhere'
                else:
                    destname = request.destination.name

                if request.block is not None:
                    name_str = request.block.full_name()
                elif request.blocks is not None:    
                    name_str = request.blocks[0].full_name()
                    for block in request.blocks[1:]:
                        name_str += '+' + block.real_name()
                else:
                    name_str = request.dataset.name

                LOG.debug('Selecting request from %s: %s to %s', plugin.name, name_str, destname)

            requests.append((request, plugin))

        return requests

    def _determine_copies(self, partition, requests):
        """
        @param partition       Partition we copy into.
        @param requests        [(DealerRequest, plugin)]
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
            'No destination available': 0,
            'Source files missing': 0
        }

        # now go through all requests
        for request, plugin in requests:
            # make sure we have all blocks complete somewhere
            if not self.policy.validate_source(request):
                reject_stats['Source files missing'] += 1
                continue

            if request.destination is None:
                # Randomly choose the destination site with probability proportional to free space
                # request.destination will be set in the function
                reject_reason = self.policy.find_destination_for(request, partition)
            else:
                # Check the destination availability
                reject_reason = self.policy.check_destination(request, partition)

            if reject_reason is not None:
                reject_stats[reject_reason] += 1
                continue

            LOG.debug('Copying %s to %s requested by %s', request.item_name(), request.destination.name, plugin.name)
            try:
                stat = stats[plugin.name][request.destination.name]
            except KeyError:
                stat = (0, 0)

            stats[plugin.name][request.destination.name] = (stat[0] + 1, stat[0] + request.item_size())

            if request.block is not None:
                blocks = [request.block]
                growing = False
            elif request.blocks is not None:
                blocks = request.blocks
                growing = False
            else:
                blocks = request.dataset.blocks
                growing = True

            new_replica = DatasetReplica(request.dataset, request.destination, growing = growing, group = request.group)
            for block in blocks:
                new_replica.block_replicas.add(BlockReplica(block, request.destination, request.group, size = 0))

            copy_list[plugin].append(new_replica)
            # New replicas may not be in the target partition, but we add the size up to be conservative
            copy_volumes[request.destination] += request.item_size()

            if not self.policy.is_target_site(request.destination.partitions[partition], copy_volumes[request.destination]):
                LOG.info('%s is not a target site any more.', request.destination.name)
                self.policy.target_sites.remove(request.destination)

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
        @param copy_list     List of dataset replicas to be created (proposal - final state depends on copy scheduling success)
        @param comment       Comment to be passed to the copy interface.
        """

        signal_blocker = SignalBlocker(logger = LOG)

        by_site = collections.defaultdict(list)
        for replica in copy_list:
            by_site[replica.site].append(replica)

        for site in sorted(by_site.iterkeys(), key = lambda s: s.name):
            replicas = by_site[site]

            LOG.info('Scheduling copy of %d replicas to %s.', len(replicas), site.name)

            with signal_blocker:
                history_record = self.history.make_cycle_entry(cycle_number, site)

                scheduled_replicas = self.copy_op.schedule_copies(replicas, history_record.operation_id, comments = comment)

                for replica in scheduled_replicas:
                    history_record.replicas.append(CopiedReplica(replica.dataset.name, replica.size(physical = False), HistoryRecord.ST_ENROUTE))

                    inventory.update(replica)
                    for block_replica in replica.block_replicas:
                        inventory.update(block_replica)

                self.history.update_entry(history_record)

                total_size = sum(r.size for r in history_record.replicas)
                LOG.info('Scheduled copy %.1f TB to %s.', total_size * 1.e-12, site.name)
