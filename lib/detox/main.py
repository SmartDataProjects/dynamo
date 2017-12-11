import time
import logging
import collections
import sys

from core.inventory import ObjectRepository
from dataformat import Dataset, DatasetReplica
from detox.policy import Protect, Delete, Dismiss, ProtectBlock, DeleteBlock, DismissBlock
from common.control import sigint

LOG = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, config, deletion_op, copy_op, history):
        """
        @param config      Configuration
        @param deletion_op An operation.deletion instance
        @param copy_op     An operation.copy instance
        @param history     A TransactionHistoryInterface instance
        """

        self.config = config
        self.deletion_op = deletion_op
        self.copy_op = copy_op
        self.history = history

    def run(self, policy, inventory, demand, comment = ''):
        """
        Main executable.

        @param policy     A Detox Policy object
        @param inventory  Dynamo inventory
        @param demand     DemandManager instance
        @param comment    Passed to dynamo history as well as the deletion interface
        """

        # fetch the deletion cycle number
        cycle_number = self.history.new_deletion_run(policy.partition.name, policy.version, is_test = self.config.local_test, comment = comment)

        LOG.info('Detox cycle %d for %s starting at %s', cycle_number, policy.partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

        # Execute the policy within a try block to avoid dead locks
        try:
            LOG.info('Building object repository for the partition.')
            partition_repository, quotas = self._build_partition(policy, inventory)

            LOG.info('Updating dataset demands.')
            demand.update(partition_repository, policy.used_demand_plugins)

            # Save the list of sites and datasets for history DB
            LOG.info('Saving site and dataset names.')
            self.history.save_sites(partition_repository.sites.values())
            self.history.save_datasets(partition_repository.datasets.values())
    
            deleted, kept, protected, reowned = self._execute_policy(policy, partition_repository, quotas)

            # insert new policy lines to the history database
            LOG.info('Saving policy conditions.')
            self.history.save_conditions(policy.policy_lines)
   
            LOG.info('Saving deletion decisions.')
            self.history.save_deletion_decisions(cycle_number, deleted, kept, protected)

            LOG.info('Saving quotas.')
            self.history.save_quotas(cycle_number, quotas)
           
            LOG.info('Committing deletion.')
            self._commit_deletions(cycle_number, policy, inventory, deleted, reowned, comment)
    
            self.history.close_deletion_run(cycle_number)
        finally:
            pass

        LOG.info('Detox cycle finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def _build_partition(self, policy, inventory):
        partition_repository = ObjectRepository()
        # Simple map of quotas
        quotas = {}

        LOG.info('Identifying target sites.')

        # Ask each site if deletion should be triggered.
        target_sites = set() # target sites of this detox cycle
        for site in inventory.sites.itervalues():
            for targdef in policy.target_site_def:
                if targdef.match(site):
                    target_sites.add(site)
                    break

        if len(target_sites) == 0:
            LOG.info('No site matches the target definition.')
            return partition_repository, quotas

        # Create a copy of the inventory, limiting to the current partition
        # We will be stripping replicas off the image as we process the policy in iterations
        LOG.info('Creating a partition image.')

        for group in inventory.groups.itervalues():
            group.embed_into(partition_repository)

        # Now clone the sites, datasets, and replicas
        # But we don't clone the site_partition because we don't need it
        for site in target_sites:
            site_clone = site.unlinked_clone()
            partition_repository.sites.add(site_clone)

            site_partition = site.partitions[policy.partition]

            for dataset_replica, block_replica_set in site_partition.replicas.iteritems():
                dataset = dataset_replica.dataset
                dataset.embed_into(partition_repository)
                for block in dataset.blocks:
                    block.embed_into(partition_repository)

                replica_clone = dataset_replica.embed_into(partition_repository)

                for block_replica in block_replica_set:
                    block_replica.block.embed_into(partition_repository)
                    block_replica.embed_into(partition_repository)

            quotas[site_clone] = site_partition.quota

        return partition_repository, quotas

    def _execute_policy(self, policy, repository, quotas):
        """
        """

        # Sites that are e.g. getting full and need dismiss calls
        triggered_sites = set()

        # We will process this list iteratively. Replicas with protection and deletion decisions are
        # taken out of the list until we are left with datasets to be dismissed only.
        all_replicas = []

        for site in repository.sites.itervalues():
            # deletion is triggered by an OR of all triggers
            for trigger in policy.deletion_trigger:
                if trigger.match(site):
                    triggered_sites.add(site)
                    break

            for replica in site.dataset_replicas():
                all_replicas.append(replica)

        LOG.info('Start deletion. Evaluating %d lines against %d replicas.', len(policy.policy_lines), len(all_replicas))

        protected = {} # {replica: {condition_id: set(block_replicas)}}
        deleted = {} # same
        kept = {} # same

        # list of block replicas that will change ownership at commit stage.
        reowned = {} # {dataset_replica: [block_replicas]}

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
        fully_protected = set()

        # now iterate through deletions, updating site usage as we go
        while True:
            iteration += 1
            LOG.info('Iteration %d, evaluating %d replicas', iteration, len(all_replicas))

            # Delete candidates: replicas that match Dismiss lines and are on sites where deletion is triggered.
            # We will only move a few replicas (on a single site up to deletion_per_iteration) from
            # delete_candidates to deleted at each iteration. The rest will be handed to keep_candidates
            delete_candidates = {} # same structure as deleted
            # Keep candidates: replicas that match Dismiss lines but are not on sites where deletion is triggered.
            # Will be passed to the kept list at the end of the final iteration.
            keep_candidates = {}

            empty_replicas = []
            start = time.time()

            for replica in all_replicas:
                # No need to reevaluate replicas that are fully protected
                if replica in fully_protected:
                    continue

                # Call policy.evaluate for each replica
                # Function evaluate() returns a list of actions. If the replica matches a dataset-level policy,
                # there is only one element in the returned list.
                # Block-level actions are triggered only if the condition does not apply to all blocks.
                # Sort the evaluation results into the three candidate containers above.

                actions = policy.evaluate(replica)

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
    
                    elif isinstance(action, DeleteBlock):
                        unlinked_replicas, reowned_replicas = self._unlink_block_replicas(replica, policy.partition, action.block_replicas)
                        if len(unlinked_replicas) != 0:
                            get_list(deleted, replica, condition_id).update(set(unlinked_replicas) - set(reowned_replicas))
                            for block_replica in unlinked_replicas:
                                block_replica.delete_from(partition_repository)

                        if len(reowned_replicas) != 0:
                            if replica in reowned:
                                reowned[replica].extend(reowned_replicas)
                            else:
                                reowned[replica] = list(reowned_replicas)

                    elif isinstance(action, DismissBlock):
                        if replica.site in triggered_sites:
                            get_list(delete_candidates, replica, condition_id).update(action.block_replicas)
                        else:
                            get_list(keep_candidates, replica, condition_id).update(action.block_replicas)

                    elif isinstance(action, Protect):
                        get_list(protected, replica, condition_id).update(replica.block_replicas)
                        fully_protected.add(replica)
    
                    elif isinstance(action, Delete):
                        unlinked_replicas, reowned_replicas = self._unlink_block_replicas(replica, policy.partition)
                        if len(unlinked_replicas) != 0:
                            get_list(deleted, replica, condition_id).update(set(unlinked_replicas) - set(reowned_replicas))
                            for block_replica in unlinked_replicas:
                                block_replica.delete_from(partition_repository)

                        if len(replica.block_replicas) == 0:
                            # if all blocks were deleted, take the replica off all_replicas for later iterations
                            # this is the only place where the replica can become empty
                            empty_replicas.append(replica)

                        if len(reowned_replicas) != 0:
                            if replica in reowned:
                                reowned[replica].extend(reowned_replicas)
                            else:
                                reowned[replica] = list(reowned_replicas)

                    elif isinstance(action, Dismiss):
                        if replica.site in triggered_sites:
                            get_list(delete_candidates, replica, condition_id).update(replica.block_replicas)
                        else:
                            get_list(keep_candidates, replica, condition_id).update(replica.block_replicas)

            for replica in empty_replicas:
                replica.delete_from(partition_repository)
                all_replicas.remove(replica)

            LOG.info('Took %f seconds to evaluate', time.time() - start)
            LOG.info(' %d dataset replicas in deletion candidates', len(delete_candidates))

            if len(delete_candidates) == 0:
                # we are done
                for replica, matches in keep_candidates.iteritems():
                    for condition_id, block_replicas in matches.iteritems():
                        get_list(kept, replica, condition_id).update(block_replicas)

                break

            else:
                # now figure out which of deletion candidates to actually delete
                if policy.iterative_deletion:
                    # we will delete from one site at a time

                    # all sites where delete candidates are
                    candidate_sites = set(r.site for r in delete_candidates.iterkeys())

                    # fraction of protected data at each candidate site
                    protected_fraction = dict((s, 0. if quotas[s] > 0. else 1.) for s in candidate_sites)

                    for replica, matches in protected.iteritems():
                        if replica.site not in protected_fraction:
                            continue

                        quota = quotas[replica.site] * 1.e+12
                        if quota <= 0.:
                            continue

                        for condition_id, block_replicas in matches.iteritems():
                            protected_fraction[replica.site] += sum(r.size for r in block_replicas) / quota

                    # find the site with the highest protected fraction                            
                    selected_site = max(candidate_sites, key = lambda site: protected_fraction[site])

                    # all delete candidates at the site
                    candidates_at_site = [r for r in delete_candidates.iterkeys() if r.site == selected_site]

                    # sorted list of replicas to delete
                    replicas_to_delete = sorted(candidates_at_site, key = policy.candidate_sort_key)

                    deleted_volume = 0.

                else:
                    replicas_to_delete = sorted(delete_candidates.iterkeys(), key = policy.candidate_sort_key)

                for replica in replicas_to_delete:
                    site = replica.site

                    if site not in triggered_sites:
                        # Site was de-triggered. Move this replica to keep_candidates.
                        for condition_id, matches in delete_candidates[replica].iteritems():
                            get_list(keep_candidates, replica, condition_id).update(matches)

                        continue

                    if policy.iterative_deletion:
                        quota = quotas[site] * 1.e+12
    
                        # have we deleted more than allowed in a single iteration?
                        if quota > 0. and deleted_volume / quota > self.config.deletion_per_iteration:
                            break

                    LOG.debug('Deleting replica: %s', str(replica))

                    for condition_id, matches in delete_candidates[replica].iteritems():
                        unlinked_replicas, reowned_replicas = self._unlink_block_replicas(replica, policy.partition, matches)
                        if len(unlinked_replicas) != 0:
                            to_delete = set(unlinked_replicas) - set(reowned_replicas)

                            if policy.iterative_deletion:
                                deleted_volume += sum(br.size for br in to_delete)

                            get_list(deleted, replica, condition_id).update(to_delete)
                            for block_replica in unlinked_replicas:
                                block_replica.delete_from(partition_repository)

                        if len(reowned_replicas) != 0:
                            if replica in reowned:
                                reowned[replica].extend(reowned_replicas)
                            else:
                                reowned[replica] = list(reowned_replicas)

                    if len(replica.block_replicas) == 0:
                        replica.delete_from(partition_repository)
                        all_replicas.remove(replica)

                    # has the site reached the stop-deletion threshold?
                    for cond in policy.stop_condition:
                        if cond.match(site):
                            triggered_sites.remove(site)
                            break

        # done iterating

        LOG.info(' %d dataset replicas in delete list', len(deleted))
        LOG.info(' %d dataset replicas in keep list', len(kept))
        LOG.info(' %d dataset replicas in protect list', len(protected))

        for line in policy.policy_lines:
            if hasattr(line, 'has_match') and not line.has_match:
                LOG.warning('Policy %s had no matching replica.' % str(line))

        # Do a last-minute check whether we can really delete these replicas
#        if policy.predelete_check is not None:
#            policy.predelete_check(list_chunk)

        return deleted, kept, protected, reowned

    def _unlink_block_replicas(self, replica, partition, block_replicas = None):
        if block_replicas is None or len(block_replicas) == len(replica.block_replicas):
            blocks_to_unlink = list(block_replicas)
            blocks_to_hand_over = []

        else:
            # Special operation - if we are deleting block replicas owned by group B, whose
            # ownership level (see dataformats/group) is Block, but the block replicas belong
            # to a dataset replica otherwise owned by group D, whose ownership level is Dataset,
            # then we don't delete the block replicas but hand them over to D.

            # establish a dataset-level owner
            dr_owner = None
            for block_replica in replica.block_replicas:
                if block_replica.group.olevel is Dataset:
                    # there is a dataset-level owner
                    dr_owner = block_replica.group
                    break

            if dr_owner is None:
                blocks_to_unlink = list(block_replicas)
                blocks_to_hand_over = []
            else:
                blocks_to_unlink = []
                blocks_to_hand_over = []
                for block_replica in block_replicas:
                    if block_replica.group.olevel is Dataset:
                        blocks_to_unlink.append(block_replica)
                    else:
                        blocks_to_hand_over.append(block_replica)

            LOG.debug('%d blocks to hand over to %s', len(blocks_to_hand_over), dr_owner.name)
            for block_replica in blocks_to_hand_over:
                block_replica.group = dr_owner

                # if the change of owner disqualifies this block replica from the partition,
                # we unlink it from the repository.
                if not partition.contains(block_replica):
                    blocks_to_unlink.append(block_replica)

        return blocks_to_unlink, blocks_to_hand_over

    def _commit_deletions(self, cycle_number, policy, inventory, deleted, reowned, comment):
        """
        @param cycle_number  Cycle number.
        @param policy        Policy object.
        @param inventory     Global (original) inventory
        @param deleted       {dataset_replica: {condition_id: set(block_replicas)}}
        @param reowned       {dataset_replica: [block_replicas]}
        @param comment       Comment to be recorded in the history DB
        """

        if not comment:
            comment = 'Dynamo -- Automatic cache release request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        # organize the replicas into sites
        deletions_by_site = collections.defaultdict(list) # {site: [(dataset_replica, block_replicas)]}
        for replica, matches in deleted.iteritems():
            all_block_replicas = set()
            for condition_id, block_replicas in matches.iteritems():
                all_block_replicas.update(block_replicas)

            deletions_by_site[replica.site].append((replica, all_block_replicas))

        # now schedule deletions for each site
        for site in sorted(deletions_by_site.iterkeys(), key = lambda s: s.name):
            site_deletion_list = deletions_by_site[site]

            LOG.info('Deleting %d replicas from %s.', len(site_deletion_list), site.name)

            sigint.block()

            flat_list = []
            for replica, block_replicas in site_deletion_list:
                if set(block_replicas) == replica.block_replicas:
                    flat_list.append(replica)
                else:
                    flat_list.extend(block_replicas)

            deletion_mapping = self.deletion_op.schedule_deletions(flat_list, comments = comment)

            total_size = 0

            for deletion_id, (approved, site, items) in deletion_mapping.iteritems():
                if not approved:
                    continue

                # Delete ownership of block replicas in the approved deletions.
                # Because replicas in partition_repository are modified already during the iterative
                # deletion, we find the original replicas from the global inventory.

                size = 0
                datasets = set()
                for item in items:
                    if type(item) is Dataset:
                        dataset = inventory.datasets[item.name]
                        replica = dataset.find_replica(site.name)
                        for block_replica in replica.block_replicas:
                            block_replica.group = inventory.groups[None]
                            inventory.update(block_replica)
                            size += block_replica.size

                        datasets.add(dataset)
                    else:
                        dataset = inventory.datasets[item.dataset.name]
                        block = dataset.find_block(item.name)
                        replica = block.find_replica(site.name)
                        replica.group = inventory.groups[None]
                        inventory.update(replica)
                        size += replica.size

                        datasets.add(dataset)

                self.history.make_deletion_entry(cycle_number, site, deletion_id, approved, datasets, size)
                total_size += size

            sigint.unblock()

            LOG.info('Done deleting %.1f TB from %s.', total_size * 1.e-12, site.name)

        # organize the replicas into sites and set up ownership change
        reown_by_site = collections.defaultdict(list) # {site: [(dataset_replica, block_replicas)]}
        for replica, block_replicas in reowned.iteritems():
            reown_by_site[replica.site].append((replica, block_replicas))

        # now schedule change of ownership (transfer request at the site) for each site
        for site in sorted(reown_by_site.iterkeys(), key = lambda s: s.name):
            site_reown_list = reown_by_site[site]

            LOG.info('Changing ownership of %d replicas at %s.', len(site_reown_list), site.name)

            sigint.block()

            flat_list = []
            for replica, block_replicas in site_reown_list:
                if set(block_replicas) == replica.block_replicas:
                    flat_list.append(replica)
                else:
                    flat_list.extend(block_replicas)

            reassigment_mapping = self.copy_op.schedule_copies(flat_list, comments = 'Dynamo -- Group reassignment')

            for copy_id, (approved, site, items) in reassignment_mapping.iteritems():
                if not approved:
                    continue

                for item in items:
                    if type(item) is Dataset:
                        dataset = inventory.datasets[item.name]
                        replica = dataset.find_replica(site.name)

                        # replica in the partition_repository
                        clone_replica = item.find_replica(site)
                        for clone_block_replica in clone_replica.block_replicas:
                            block_replica = replica.find_block_replica(clone_block_replica.block.name)

                            block_replica.group = inventory.groups[clone_block_replica.group.name]
                            inventory.update(block_replica)
                    else:
                        dataset = inventory.datasets[item.dataset.name]
                        block = dataset.find_block(item.name)
                        replica = block.find_replica(site.name)

                        replica.group = inventory.groups[item.group.name]
                        inventory.update(replica)

            sigint.unblock()
