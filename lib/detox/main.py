import time
import logging
import collections

from dynamo.core.inventory import ObjectRepository
from dynamo.dataformat import Group, Site, Dataset, Block, DatasetReplica, BlockReplica
from dynamo.dataformat.history import DeletedReplica
from dynamo.detox.detoxpolicy import DetoxPolicy
from dynamo.detox.detoxpolicy import Ignore, Protect, Delete, Dismiss, ProtectBlock, DeleteBlock, DismissBlock
from dynamo.detox.history import DetoxHistory
from dynamo.operation.deletion import DeletionInterface
from dynamo.utils.signaling import SignalBlocker

LOG = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, config):
        """
        @param config      Configuration
        """

        if 'deletion_op' in config:
            self.deletion_op = DeletionInterface.get_instance(config.deletion_op.module, config.deletion_op.config)
        else:
            self.deletion_op = DeletionInterface.get_instance()

        self.history = DetoxHistory(config.get('history', None))

        self.policy = DetoxPolicy(config)

        self.deletion_per_iteration = config.deletion_per_iteration

        self.test_run = config.get('test_run', False)
        if self.test_run:
            self.deletion_op.set_read_only()

    def set_read_only(self, value = True):
        self.deletion_op.set_read_only(value)
        self.history.set_read_only(value)

    def run(self, inventory, comment = '', create_cycle = True):
        """
        Main executable.
        @param inventory    Dynamo inventory
        @param comment      Passed to dynamo history
        @param create_cycle If True, assign a cycle number and make a permanent record in the history.
        """

        if create_cycle:
            # fetch the deletion cycle number
            cycle_tag = self.history.new_cycle(self.policy.partition_name, self.policy.version, comment = comment, test = self.test_run)
            LOG.info('Detox cycle %d for %s starting', cycle_tag, self.policy.partition_name)
        else:
            cycle_tag = self.policy.partition_name
            LOG.info('Detox snapshot cycle for %s starting', self.policy.partition_name)

        LOG.info('Building the object repository for the partition.')
        # Create a full clone of the inventory limited to the partition of the policy
        partition_repository = self._build_partition(inventory)

        LOG.info('Loading dataset attributes.')
        for plugin in self.policy.attr_producers:
            plugin.load(partition_repository)

        LOG.info('Saving policy conditions.')
        # Sets policy IDs for each lines from the history DB; need to run this before execute_policy
        self.history.save_conditions(self.policy.policy_lines)

        LOG.info('Applying policy to replicas.')
        deleted, kept, protected, reowned = self._execute_policy(partition_repository)

        partition = partition_repository.partitions[self.policy.partition_name]
        quotas = dict((s, s.partitions[partition].quota * 1.e-12) for s in partition_repository.sites.itervalues())

        LOG.info('Saving deletion decisions and site states.')
        self.history.save_cycle_state(cycle_tag, deleted, kept, protected, quotas)

        if create_cycle:
            LOG.info('Committing deletion.')
            comment = 'Dynamo -- Automatic cache release request for %s partition.' % self.policy.partition_name
            self._commit_deletions(cycle_tag, inventory, deleted, comment)
            comment = 'Dynamo -- Automatic group reassignment for %s partition.' % self.policy.partition_name
            self._commit_reassignments(inventory, reowned, comment)

            self.history.close_cycle(cycle_tag)

        LOG.info('Detox cycle completed')

    def _build_partition(self, inventory):
        """Create a mini-inventory consisting only of replicas in the partition."""

        partition_repository = ObjectRepository()
        partition_repository._store = inventory._store

        LOG.info('Identifying target sites.')

        partition = inventory.partitions[self.policy.partition_name]

        partition.embed_tree(partition_repository)

        # Ask each site if deletion should be triggered.
        target_sites = set() # target sites of this detox cycle
        tape_is_target = False
        for site in inventory.sites.itervalues():
            # target_site_defs are SiteConditions, which take site_partition as the argument
            site_partition = site.partitions[partition]

            for targdef in self.policy.target_site_def:
                if targdef.match(site_partition):
                    target_sites.add(site)
                    if site.storage_type == Site.TYPE_MSS:
                        tape_is_target = True

                    break

        if len(target_sites) == 0:
            LOG.info('No site matches the target definition.')
            return partition_repository

        # Safety measure - if there are empty (no block rep) tape replicas, create block replicas with size 0 and
        # add them into the partition. We will not report back to the main process though (i.e. won't call inventory.update).
        if tape_is_target:
            for site in filter(lambda s: s.storage_type == Site.TYPE_MSS, target_sites):
                for replica in site.dataset_replicas():
                    if len(replica.block_replicas) != 0:
                        continue

                    for block in replica.dataset.blocks:
                        block_replica = BlockReplica(block, site, Group.null_group, size = 0)
                        replica.block_replicas.add(block_replica)
                        block.replicas.add(block_replica)

                    # Add to the site partition
                    site.partitions[partition].replicas[replica] = None

        # Create a copy of the inventory, limiting to the current partition
        # We will be stripping replicas off the image as we process the policy in iterations
        LOG.info('Creating a partition image.')

        for group in inventory.groups.itervalues():
            group.embed_into(partition_repository)

        # Now clone the sites, datasets, and replicas
        # Basically a copy-paste of various embed_into() functions ommitting the checks

        # make a map to avoid excessive lookups
        block_to_clone = {}
        for site in target_sites:
            site_clone = site.embed_into(partition_repository)

            site_partition = site.partitions[partition]
            site_partition_clone = site_partition.embed_tree(partition_repository)

            for dataset_replica, block_replica_set in site_partition.replicas.iteritems():
                dataset = dataset_replica.dataset

                try:
                    dataset_clone = partition_repository.datasets[dataset.name]

                except KeyError:
                    dataset_clone = dataset.embed_into(partition_repository)

                    for block in dataset.blocks:
                        block_clone = Block(
                            block.name,
                            dataset_clone,
                            size = block.size,
                            num_files = block.num_files,
                            is_open = block.is_open,
                            last_update = block.last_update,
                            bid = block.id
                        )
                        dataset_clone.blocks.add(block_clone)

                        block_to_clone[block] = block_clone

                if dataset_replica.group is None:
                    group = None
                else:
                    group = partition_repository.groups[dataset_replica.group.name]

                replica_clone = DatasetReplica(
                    dataset_clone,
                    site_clone,
                    growing = dataset_replica.growing,
                    group = group
                )
                dataset_clone.replicas.add(replica_clone)
                site_clone.add_dataset_replica(replica_clone, add_block_replicas = False)

                if block_replica_set is None:
                    # all block reps in partition
                    block_replica_set = dataset_replica.block_replicas
                    full_replica = True
                    site_partition_clone.replicas[replica_clone] = None
                else:
                    full_replica = False
                    block_replica_clone_set = site_partition_clone.replicas[replica_clone] = set()

                for block_replica in block_replica_set:
                    block_clone = block_to_clone[block_replica.block]
                    if block_replica.is_complete():
                        size = -1
                    else:
                        size = block_replica.size

                    block_replica_clone = BlockReplica(
                        block_clone,
                        site_clone,
                        partition_repository.groups[block_replica.group.name],
                        is_custodial = block_replica.is_custodial,
                        size = size,
                        last_update = block_replica.last_update,
                        file_ids = block_replica.file_ids
                    )

                    replica_clone.block_replicas.add(block_replica_clone)
                    block_clone.replicas.add(block_replica_clone)

                    if not full_replica:
                        block_replica_clone_set.add(block_replica_clone)

        return partition_repository

    def _execute_policy(self, repository):
        """
        Sort replicas into deleted, kept, protected, and reowned according to the policy.
        The lists deleted/kept/protected are disjoint. Reowned list overlaps with others.
        """

        partition = repository.partitions[self.policy.partition_name]

        # Sites that are e.g. getting full and need dismiss calls
        triggered_sites = set()

        # Site -> partition quota
        quotas = {}

        # We will process this list iteratively. Replicas with protection and deletion decisions are
        # taken out of the list until we are left with datasets to be dismissed only.
        all_replicas = set()

        for site in repository.sites.itervalues():
            site_partition = site.partitions[partition]
            # deletion is triggered by an OR of all triggers
            for trigger in self.policy.deletion_trigger:
                if trigger.match(site_partition):
                    triggered_sites.add(site)
                    break

            quotas[site] = site.partitions[partition].quota

            for replica in site.dataset_replicas():
                all_replicas.add(replica)

        LOG.info('Start deletion. Evaluating %d lines against %d replicas.', len(self.policy.policy_lines), len(all_replicas))

        protected = {} # {replica: {condition_id: set(block_replicas)}}
        deleted = {} # same
        kept = {} # same

        # list of block replicas that will change ownership at commit stage.
        reowned = {} # {dataset_replica: set([block_replicas])}

        def get_list(outmap, replica, condition_id):
            try:
                replica_map = outmap[replica]
            except KeyError:
                replica_map = outmap[replica] = {}
            try:
                return replica_map[condition_id]
            except KeyError:
                s = replica_map[condition_id] = set()
                return s

        iteration = 0

        # now iterate through deletions, updating site usage as we go
        while True:
            iteration += 1
            LOG.info('Iteration %d, evaluating %d replicas', iteration, len(all_replicas))

            # Delete candidates: replicas that match Dismiss lines and are on sites where deletion is triggered.
            # We will only move a few replicas (on a single site up to deletion_per_iteration) from
            # delete_candidates to deleted at each iteration. The rest will be handed to keep_candidates
            delete_candidates = {} # same structure as deleted
            # For replicas that were Dismissed at dataset level, if it ends up being flagged for deletion,
            # we perform a dataset-level deletion (set growing to False and delete the DatasetReplica in addition
            # to BlockReplicas).
            dataset_level_delete_candidates = set()
            # Keep candidates: replicas that match Dismiss lines but are not on sites where deletion is triggered.
            # Will be passed to the kept list at the end of the final iteration.
            keep_candidates = {}

            ignored_replicas = set()
            empty_replicas = set()
            start = time.time()

            for replica in all_replicas:
                # Call policy.evaluate for each replica
                # Function evaluate() returns a list of actions. If the replica matches a dataset-level policy,
                # there is only one element in the returned list.
                # Block-level actions are triggered only if the condition does not apply to all blocks.
                # Sort the evaluation results into the three candidate containers above.
                actions = self.policy.evaluate(replica)

                # Keep track of block replicas matching block-level conditions
                block_replicas = set(replica.block_replicas)

                # Block-level actions come first - take out all blocks that matched some condition.
                # Remaining block replicas are the ones the dataset-level action applies to.
                for action in actions:
                    matched_line = action.matched_line

                    if matched_line is None:
                        condition_id = 0
                    else:
                        condition_id = matched_line.condition_id

                    if isinstance(action, ProtectBlock):
                        get_list(protected, replica, condition_id).update(action.block_replicas)
                        block_replicas -= action.block_replicas
    
                    elif isinstance(action, DeleteBlock):
                        # blocks sorted into ones to be unlinked and ones to be reowned
                        # ones to be unlinked are removed from block_replicas
                        # ones to be reowned are added to reowned
                        # the two sets overlap only when reowning causes the block replica to go out of the partition
                        # unlinked - reowned are returned as to_delete
                        to_delete = self._unlink_block_replicas(replica, partition, action.block_replicas, repository, reowned, block_replicas)

                        if len(to_delete) != 0:
                            # to_delete list contains blocks that should actually be deleted, instead of just kicked out
                            # from the repository
                            get_list(deleted, replica, condition_id).update(to_delete)

                    elif isinstance(action, DismissBlock):
                        if replica.site in triggered_sites:
                            get_list(delete_candidates, replica, condition_id).update(action.block_replicas)
                        else:
                            get_list(keep_candidates, replica, condition_id).update(action.block_replicas)

                        block_replicas -= action.block_replicas

                    elif isinstance(action, Ignore):
                        ignored_replicas.add(replica)

                    elif isinstance(action, Protect):
                        # protect a full dataset or a remainder after block-level operations
                        get_list(protected, replica, condition_id).update(block_replicas)
                        if block_replicas == replica.block_replicas:
                            # if all block replicas are to be protected, we don't need to evaluate this dataset replica any more.
                            # add to the ignore list to speed up processing
                            ignored_replicas.add(replica)
    
                    elif isinstance(action, Delete):
                        # delete a full dataset or a remainder after block-level operations
                        to_delete = self._unlink_block_replicas(replica, partition, block_replicas, repository, reowned)

                        if len(to_delete) != 0:
                            get_list(deleted, replica, condition_id).update(to_delete)

                        # as a result of the modification, the dataset replica can become empty
                        if len(replica.block_replicas) == 0:
                            # replica is deleted at dataset level - can no longer be growing
                            replica.growing = False
                            # if all blocks were deleted, take the replica off all_replicas for later iterations
                            # this is the only place where the replica can become empty
                            empty_replicas.add(replica)

                    elif isinstance(action, Dismiss):
                        if replica.site in triggered_sites:
                            get_list(delete_candidates, replica, condition_id).update(block_replicas)
                            dataset_level_delete_candidates.add(replica)
                        else:
                            get_list(keep_candidates, replica, condition_id).update(block_replicas)

            for replica in empty_replicas:
                replica.unlink_from(repository)

            all_replicas -= empty_replicas
            all_replicas -= ignored_replicas

            LOG.info('Took %f seconds to evaluate', time.time() - start)
            LOG.info(' %d dataset replicas in deletion candidates', len(delete_candidates))

            if len(delete_candidates) == 0:
                # we are done
                for replica, matches in keep_candidates.iteritems():
                    for condition_id, block_replicas in matches.iteritems():
                        get_list(kept, replica, condition_id).update(block_replicas)

                break

            # now figure out which of deletion candidates to actually delete
            if self.policy.iterative_deletion:
                # we will delete from one site at a time

                # all sites where delete candidates are
                candidate_sites = set(r.site for r in delete_candidates.iterkeys())

                # fraction of protected data at each candidate site
                protected_fraction = dict((s, 0. if quotas[s] > 0. else 1.) for s in candidate_sites)

                for replica, matches in protected.iteritems():
                    if replica.site not in protected_fraction:
                        continue

                    quota = quotas[replica.site]
                    if quota <= 0.:
                        continue

                    for condition_id, block_replicas in matches.iteritems():
                        protected_fraction[replica.site] += sum(r.size for r in block_replicas) / quota

                # find the site with the highest protected fraction                            
                selected_site = max(candidate_sites, key = lambda site: protected_fraction[site])

                # all delete candidates at the site
                candidates_at_site = [r for r in delete_candidates.iterkeys() if r.site == selected_site]

                # sorted list of replicas to delete
                replicas_to_delete = sorted(candidates_at_site, key = self.policy.candidate_sort_key)

                deleted_volume = 0.

            else:
                replicas_to_delete = sorted(delete_candidates.iterkeys(), key = self.policy.candidate_sort_key)

            for replica in replicas_to_delete:
                site = replica.site

                if site not in triggered_sites:
                    # Site was de-triggered. Move this replica to keep_candidates.
                    for condition_id, matches in delete_candidates[replica].iteritems():
                        get_list(keep_candidates, replica, condition_id).update(matches)

                    continue

                if self.policy.iterative_deletion:
                    quota = quotas[site]

                    # have we deleted more than allowed in a single iteration?
                    if quota > 0. and deleted_volume / quota > self.deletion_per_iteration:
                        break

                LOG.debug('Deleting replica: %s', str(replica))

                for condition_id, matches in delete_candidates[replica].iteritems():
                    to_delete = self._unlink_block_replicas(replica, partition, matches, repository, reowned)

                    if len(to_delete) != 0:
                        get_list(deleted, replica, condition_id).update(to_delete)

                        if self.policy.iterative_deletion:
                            deleted_volume += sum(br.size for br in to_delete)

                if len(replica.block_replicas) == 0:
                    if replica in dataset_level_delete_candidates:
                        replica.growing = False
                    
                    replica.unlink_from(repository)
                    all_replicas.remove(replica)

                site_partition = site.partitions[partition]

                # has the site reached the stop-deletion threshold?
                for cond in self.policy.stop_condition:
                    if cond.match(site_partition):
                        triggered_sites.remove(site)
                        break

        # done iterating

        LOG.info(' %d dataset replicas in delete list', len(deleted))
        LOG.info(' %d dataset replicas in keep list', len(kept))
        LOG.info(' %d dataset replicas in protect list', len(protected))

        for line in self.policy.policy_lines:
            if not line.has_match:
                LOG.warning('Policy %s had no matching replica.' % str(line))

        # Do a last-minute check whether we can really delete these replicas
#        if policy.predelete_check is not None:
#            policy.predelete_check(list_chunk)

        return deleted, kept, protected, reowned

    def _unlink_block_replicas(self, replica, partition, block_replicas, repository, reowned, remaining_block_replicas = None):
        if block_replicas is None or len(block_replicas) == len(replica.block_replicas):
            blocks_to_unlink = set(replica.block_replicas)
            blocks_to_hand_over = set()

        else:
            # Special operation - if we are deleting block replicas owned by group B, whose
            # ownership level (see dataformats/group) is Block, but the block replicas belong
            # to a dataset replica otherwise owned by group D, whose ownership level is Dataset,
            # then we don't delete the block replicas but hand them over to D.

            # establish a dataset-level owner
            dr_owner = None
            for block_replica in replica.block_replicas:
                if block_replica.group.olevel == Group.OL_DATASET:
                    # there is a dataset-level owner
                    dr_owner = block_replica.group
                    break

            if dr_owner is None:
                blocks_to_unlink = set(block_replicas)
                blocks_to_hand_over = set()

                LOG.debug('All blocks to unlink in %s', len(blocks_to_hand_over), str(replica))
            else:
                blocks_to_unlink = set()
                blocks_to_hand_over = set()
                for block_replica in block_replicas:
                    if block_replica.group.olevel == Group.OL_DATASET:
                        blocks_to_unlink.add(block_replica)
                    else:
                        blocks_to_hand_over.add(block_replica)

                LOG.debug('%d blocks to hand over to %s in %s', len(blocks_to_hand_over), dr_owner.name, str(replica))

                for block_replica in blocks_to_hand_over:
                    block_replica.group = dr_owner
    
                    # if the change of owner disqualifies this block replica from the partition,
                    # we unlink it from the repository.
                    if not partition.contains(block_replica):
                        blocks_to_unlink.add(block_replica)

        if len(blocks_to_unlink) != 0:
            for block_replica in blocks_to_unlink:
                block_replica.unlink_from(repository)

            # if this replica was put in reowned list earlier, take it out
            try:
                reowned_replicas = reowned[replica]
            except KeyError:
                pass
            else:
                for block_replica in blocks_to_unlink:
                    try:
                        reowned_replicas.remove(block_replica)
                    except KeyError:
                        pass

                if len(reowned_replicas) == 0:
                    reowned.pop(replica)

            # if called when encountering a DeleteBlock operation, take these blocks out from consideration in further iterations
            if remaining_block_replicas is not None:
                remaining_block_replicas -= blocks_to_unlink

        if len(blocks_to_hand_over) != 0:
            if replica in reowned:
                reowned[replica].extend(blocks_to_hand_over)
            else:
                reowned[replica] = blocks_to_hand_over

        return blocks_to_unlink - blocks_to_hand_over

    def _commit_deletions(self, cycle_number, inventory, deleted, comment):
        """
        @param cycle_number  Cycle number.
        @param inventory     Global (original) inventory
        @param deleted       {dataset_replica: {condition_id: set(block_replicas)}}
        @param comment       Comment to be passed to the deletion interface.
        """

        signal_blocker = SignalBlocker(logger = LOG)

        # get the original replicas from the inventory and organize them into sites
        deletions_by_site = collections.defaultdict(list) # {site: [(dataset_replica, block_replicas)]}

        for replica, matches in deleted.iteritems():
            site = inventory.sites[replica.site.name]

            original_replica = inventory.datasets[replica.dataset.name].find_replica(site)
            original_block_replicas = dict((br.block.name, br) for br in original_replica.block_replicas)
            
            all_block_replicas = set()
            for block_replicas in matches.itervalues():
                for block_replica in block_replicas:
                    all_block_replicas.add(original_block_replicas[block_replica.block.name])

            if not replica.growing and all_block_replicas == original_replica.block_replicas:
                # if we are deleting all block replicas and the replica is marked as not growing, delete the DatasetReplica
                deletions_by_site[site].append((original_replica, None))
            else:
                # otherwise delete only the BlockReplicas
                deletions_by_site[site].append((original_replica, list(all_block_replicas)))

        # now schedule deletions for each site
        for site in sorted(deletions_by_site.iterkeys(), key = lambda s: s.name):
            site_deletion_list = deletions_by_site[site]

            LOG.info('Deleting %d replicas from %s.', len(site_deletion_list), site.name)

            # Block interruptions until deletion is executed and recorded
            with signal_blocker:
                history_record = self.history.make_cycle_entry(cycle_number, site)

                scheduled_replicas = self.deletion_op.schedule_deletions(site_deletion_list, history_record.operation_id, comments = comment)

                for replica, block_replicas in scheduled_replicas:
                    # replicas are clones -> use inventory.update instead of inventory.register_update

                    if block_replicas is None:
                        replica.growing = False
                        replica.group = inventory.groups[None]
                        inventory.update(replica)
                        block_replicas = replica.block_replicas

                    deleted_size = 0

                    for block_replica in block_replicas:
                        block_replica.group = inventory.groups[None]
                        inventory.update(block_replica)

                        deleted_size += block_replica.size

                    history_record.replicas.append(DeletedReplica(replica.dataset.name, deleted_size))

                self.history.update_entry(history_record)

                total_size = sum(r.size for r in history_record.replicas)
                LOG.info('Done deleting %.1f TB from %s.', total_size * 1.e-12, site.name)

    def _commit_reassignments(self, inventory, reowned, comment):
        """
        @param inventory     Global (original) inventory
        @param reowned       {dataset_replica: set([block_replicas])}
        @param comment       Comment to be passed to the copy interface.
        """

        # If Dynamo owns all files, all we need to do is update the inventory.
        # It is however possible that the other hand the underlying storage system requires some operation.
        need_operation = hasattr(self.deletion_op, 'schedule_reassignments')

        if need_operation:
            # get the original replicas from the inventory and organize them into sites
            reown_by_site = collections.defaultdict(list) # {site: [(dataset_replica, block_replicas)]}

        for replica, block_replicas in reowned.iteritems():
            # just do the reassignment in the inventory upfront
            original_replica = inventory.update(replica)

            original_block_replicas = dict((br.block.name, br) for br in original_replica.block_replicas)

            all_block_replicas = set()
            for block_replica in block_replicas:
                original_block_replica = original_block_replicas[block_replica.block.name]

                if original_block_replica != block_replica:
                    original_block_replica.copy(block_replica)
                    inventory.register_update(original_block_replica)

                all_block_replicas.add(original_block_replica)

            if need_operation:
                if replica.growing and all_block_replicas == original_replica.block_replicas:
                    # if we are reassigning all block replicas and the replica is marked as growing, reassign the DatasetReplica
                    reown_by_site[original_replica.site].append((original_replica, None))
                else:
                    # otherwise reassign by BlockReplicas
                    reown_by_site[original_replica.site].append((original_replica, all_block_replicas))

        if need_operation:
            for site in sorted(reown_by_site.iterkeys(), key = lambda s: s.name):
                site_reown_list = reown_by_site[site]
    
                LOG.info('Changing ownership of %d replicas at %s.', len(site_reown_list), site.name)
    
                self.deletion_op.schedule_reassignments(site_reown_list, comments = comment)
