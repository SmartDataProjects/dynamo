import time
import logging
import math
import pprint
import collections
import random
import sys
import os

import common.configuration as config
from common.dataformat import Dataset, Block, Site
from policy import Protect, Delete, Dismiss, ProtectBlock, DeleteBlock, DismissBlock
import detox.configuration as detox_config
from common.misc import timer, parallel_exec, sigint

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, history):
        """
        @param inventory   An InventoryManager instance
        @param transaction A TransactionManager instance
        @param demand      A DemandManager instance
        @param history     A TransactionHistoryInterface instance
        """

        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

    def run(self, policy, is_test = False, comment = ''):
        """
        Main executable.

        @param policy     A Detox Policy object
        @param is_test    Set to True when e.g. the main binary is invoked with --test-run option.
        @param comment    Passed to dynamo history as well as the deletion interface
        """

        logger.info('Detox cycle for %s starting at %s', policy.partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

        # Execute the policy within a try block to avoid dead locks
        try:
            # insert new policy lines to the history database
            logger.info('Saving policy conditions.')
            self.history.save_conditions(policy.policy_lines)
    
            logger.info('Updating dataset demands.')
            # update requests, popularity, and locks
            self.demand_manager.update(self.inventory_manager, policy.used_demand_plugins)
    
            logger.info('Updating site status.')
            # update site status
            self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates
    
            self._execute_policy(policy, is_test, comment)

        finally:
            pass

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def _execute_policy(self, policy, is_test, comment):
        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        logger.info('Preparing deletion run %d', run_number)

        logger.info('Identifying target sites.')

        # Ask each site if deletion should be triggered.
        target_sites = set() # target sites of this detox cycle
        triggered_sites = set() # sites that are e.g. getting full and need dismiss calls
        for site in self.inventory_manager.sites.itervalues():
            if policy.target_site_def.match(site):
                target_sites.add(site)
                if policy.deletion_trigger.match(site):
                    triggered_sites.add(site)

        if len(target_sites) == 0:
            logger.info('No site matches the target definition.')
            return

        quotas = dict((s, s.partition_quota(policy.partition)) for s in target_sites)

        logger.info('Identifying dataset replicas in the partition.')

        # "partition" as a verb - selecting only the blockreps in the partition
        # will also select out replicas on sites with quotas
        all_replicas = policy.partition_replicas(self.inventory_manager, target_sites)

        logger.info('Saving site and dataset states.')

        # update site and dataset lists
        self.history.save_sites(target_sites)
        self.history.save_datasets(set(r.dataset for r in all_replicas))

        logger.info('Start deletion. Evaluating %d lines against %d replicas.', len(policy.policy_lines), len(all_replicas))

        if policy.need_iteration:
            # if quota is 0, protected fraction is identically 1
            protected_fraction = dict((s, 1. if q == 0 else 0.) for s, q in quotas.iteritems())

        protected = collections.defaultdict(list) # {replica: [condition_id or ([block_replica], condition_id)]}
        deleted = collections.defaultdict(list) # same
        kept = collections.defaultdict(list) # same

        def apply_delete(replica, condition):
            logger.debug('apply_delete: %s %s, condition %d', replica.site.name, replica.dataset.name, condition)

            # We need to detach the replica from owning containers (dataset and site) for policy evaluation
            # in the later iterations - will be relinked if deletion fails in commit_deletion
            replica.unlink()

            deleted[replica].append(condition)
            all_replicas.remove(replica)

            return replica.size()

        def apply_deleteblock(replica, block_replicas, condititon):
            # Special operation - if we are deleting block replicas owned by group B, whose
            # ownership level (see dataformats/group) is Block, but the block replicas belong
            # to a dataset replica otherwise owned by group D, whose ownership level is Dataset,
            # then we don't delete the block replicas but hand them over to D.

            logger.debug('apply_deleteblock: %s %s %d blocks, condition %d', replica.site.name, replica.dataset.name, len(block_replicas), condition)

            # establish a dataset-level owner
            dr_owner = None
            for block_replica in replica.block_replicas:
                if block_replica.group.olevel is Dataset:
                    # there is a dataset-level owner
                    dr_owner = block_replica.group
                    break

            if dr_owner is None:
                blocks_to_hand_over = []
                blocks_to_delete = list(block_replicas)
            else:
                blocks_to_hand_over = []
                blocks_to_delete = []
                for block_replica in block_replicas:
                    if block_replica.group.olevel is Dataset:
                        blocks_to_delete.append(block_replica)
                    else:
                        blocks_to_hand_over.append(block_replica)

            if len(blocks_to_hand_over) != 0:
                logger.debug('%d blocks to hand over to %s', len(blocks_to_hand_over), dr_owner.name)
                # not ideal to make reassignments here, but this operation affects later iterations
                self.reassign_owner(replica, blocks_to_hand_over, dr_owner, policy.partition, is_test)

            if len(blocks_to_delete) != 0:
                logger.debug('%d blocks to delete', len(blocks_to_delete))
                deleted[replica].append((blocks_to_delete, condition))

                for block_replica in blocks_to_delete:
                    replica.site.remove_block_replica(block_replica)

            return sum(br.size for br in blocks_to_delete)


        iteration = 0

        # now iterate through deletions, updating site usage as we go
        # if policy.need_iteration is False, break after first pass
        while True:
            if policy.need_iteration:
                iteration += 1
                logger.info('Iteration %d, evaluating %d replicas', iteration, len(all_replicas))

            # call policy.evaluate for each replica
            # evaluate() returns a list of actions. If the replica matches a dataset-level policy, there is only one element in the returned list.
            start = time.time()
            eval_results = []
            for replica in all_replicas:
                eval_results.extend(policy.evaluate(replica))

            logger.info('Took %f seconds to evaluate', time.time() - start)

            delete_candidates = collections.defaultdict(list) # {replica: [condition_id or ([block_replica], condition_id)]}
            keep_candidates = collections.defaultdict(list)

            protect_sizes = collections.defaultdict(int)

            # Sort the evaluation results into containers
            # Block-level actions are triggered only if the condition does not apply to all blocks
            # Policy object issues a dataset-level action otherwise
            for action in eval_results:
                replica = action.replica
                condition = action.condition

                if isinstance(action, Protect):
                    protected[replica].append(condition)
                    all_replicas.remove(replica)

                    size = replica.size()

                    if policy.need_iteration:
                        protect_sizes[replica.site] += size

                elif isinstance(action, Delete):
                    apply_delete(replica, condition)

                elif isinstance(action, Dismiss):
                    if replica.site in triggered_sites:
                        delete_candidates[replica].append(condition)
                    else:
                        keep_candidates[replica].append(condition)

                elif isinstance(action, ProtectBlock):
                    protected[replica].append((action.block_replicas, condition))

                    size = sum(br.size for br in action.block_replicas)

                    if policy.need_iteration:
                        protect_sizes[replica.site] += size

                elif isinstance(action, DeleteBlock):
                    apply_deleteblock(replica, action.block_replicas, condition)

                elif isinstance(action, DismissBlock):
                    if replica.site in triggered_sites:
                        delete_candidates[replica].append((action.block_replicas, condition))
                    else:
                        keep_candidates[replica].append((action.block_replicas, condition))


            logger.info(' %d dataset replicas in deletion candidates', len(delete_candidates))
            logger.info(' %d dataset replicas in deletion list', len(deleted))
            logger.info(' %d dataset replicas in protection list', len(protected))

            if len(delete_candidates) == 0:
                # no more deletion candidates
                kept.update(keep_candidates)
                break

            # now figure out which of deletion candidates to actually delete
            # first determine which sites to process

            if policy.need_iteration:
                # iterative deletion happens at one site at a time

                # first update the protected fractions
                for site, size in protect_sizes.iteritems():
                    quota = quotas[site] * 1.e+12
                    if quota > 0.:
                        protected_fraction[site] += size / quota

                candidate_sites = set(r.site for r in delete_candidates.iterkeys())
        
                if len(protect_sizes) != 0:
                    # find the site with the highest protected fraction
                    selected_site = max(candidate_sites, key = lambda site: protected_fraction[site])
                else:
                    selected_site = random.choice(list(candidate_sites))

                candidates_at_site = [r for r in delete_candidates.iterkeys() if r.site == selected_site]
                replicas_to_delete = sorted(candidates_at_site, key = policy.candidate_sort_key)

            else:
                replicas_to_delete = delete_candidates.keys()

            deleted_volume = collections.defaultdict(float)

            for replica in replicas_to_delete:
                site = replica.site

                if policy.stop_condition.match(site):
                    continue

                quota = quotas[site] * 1.e+12

                if policy.need_iteration and quota > 0. and \
                        deleted_volume[site] / quota > detox_config.main.deletion_per_iteration:
                    continue

                if logger.getEffectiveLevel() == logging.DEBUG:
                    logger.debug('Deleting replica: %s', str(replica))

                matches = delete_candidates.pop(replica)

                for match in matches:
                    if type(match) is tuple: # block-level, ([block_replica], condition)
                        deleted_volume[site] += apply_deleteblock(replica, match[0], match[1])
                    else:
                        deleted_volume[site] += apply_delete(replica, match)

            # Iterative deletion -> remaining replicas in delete_candidates are still in all_replicas
            # and will be re-evaluated in the next iteration
            # Non-iterative deletion -> remaining replicas in delete_candidates are all to be kept

            if not policy.need_iteration:
                # we are done
                kept.update(keep_candidates)
                kept.update(delete_candidates)
                break

            # update the list of target sites
            for site in list(triggered_sites):
                if policy.stop_condition.match(site):
                    triggered_sites.remove(site)

        # done iterating

        for line in policy.policy_lines:
            if hasattr(line, 'has_match') and not line.has_match:
                logger.warning('Policy %s had no matching replica.' % str(line))

        # save replica snapshots and all deletion decisions
        logger.info('Saving deletion decisions.')

        self.history.save_deletion_decisions(run_number, quotas, deleted, kept, protected)
        
        logger.info('Committing deletion.')

        # we have recorded deletion reasons; we can now consolidate deleted block replicas

        deletion_list = []
        for replica, matches in deleted.iteritems():
            for match in matches:
                if type(match) is tuple:
                    replica.block_replicas.extend(match[0])

            deletion_list.append(replica)

        self.commit_deletions(run_number, policy, deletion_list, is_test, comment)

        logger.info('Restoring inventory state.')

        # recover fragmented dataset replicas
        for replica, matches in protected.iteritems():
            for match in matches:
                if type(match) is tuple:
                    replica.block_replicas.extend(match[0])

        for replica, matches in kept.iteritems():
            for match in matches:
                if type(match) is tuple:
                    replica.block_replicas.extend(match[0])

        # then bring back replicas not in the partition
        policy.restore_replicas()

        self.history.close_deletion_run(run_number)

    def reassign_owner(self, dataset_replica, block_replicas, new_owner, partition, is_test):
        """
        Add back the block replicas to dataset replica under the new owner.
        """

        self.transaction_manager.copy.schedule_reassignments(block_replicas, new_owner, comments = 'Dynamo -- Group reassignment', is_test = is_test)

        site = dataset_replica.site

        new_replicas = []
        for old_replica in block_replicas:
            site.remove_block_replica(old_replica)

            new_replica = old_replica.clone(group = new_owner)

            dataset_replica.block_replicas.append(new_replica)
            site.add_block_replica(new_replica, partitions = [partition])
            
            new_replicas.append(new_replica)

        if not is_test:
            # are we relying on do_update = True in insert_many <- add_blockreplicas here?
            self.inventory_manager.store.add_blockreplicas(new_replicas)

    def commit_deletions(self, run_number, policy, deletion_list, is_test, comment):
        """
        @param run_number    Cycle number.
        @param policy        Policy object.
        @param deletion_list List of dataset replicas (can be partial) to be deleted.
        @param is_test       Do not actually delete if True
        @param comment       Comment to be recorded in the history DB
        """

        if not comment:
            comment = 'Dynamo -- Automatic cache release request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        # organize the replicas into sites and set up block-level deletions
        deletions_by_site = collections.defaultdict(list)
        for replica in deletion_list:
            deletions_by_site[replica.site].append(replica)

        # now schedule deletion for each site
        for site in sorted(deletions_by_site.iterkeys()):
            if site.storage_type == Site.TYPE_MSS:
                if config.daemon_mode:
                    logger.warning('Deletion from MSS cannot be done in daemon mode.')
                    continue
            
                print 'Deletion from', site.name, 'is requested. Are you sure? [Y/n]'
                response = sys.stdin.readline().strip()
                if response != 'Y':
                    logger.warning('Aborting.')
                    continue

            site_deletion_list = deletions_by_site[site]

            logger.info('Deleting %d replicas from %s.', len(site_deletion_list), site.name)

            sigint.block()

            deletion_mapping = {} #{deletion_id: (approved, [replicas])}

            chunk_size = detox_config.main.deletion_volume_per_request * 1.e+12

            while len(site_deletion_list) != 0:
                # stack up replicas up to 110% of volume_per_request
                # unnecessary complication in my mind, but has been requested
                list_chunk = []
                list_above_chunk = []
                deletion_size = 0
                while len(site_deletion_list) != 0:
                    size = site_deletion_list[-1].size()

                    if deletion_size > chunk_size and deletion_size + size > chunk_size * 1.1:
                        # put the excess back
                        list_above_chunk.reverse()
                        site_deletion_list.extend(list_above_chunk)
                        list_above_chunk = []
                        break

                    replica = site_deletion_list.pop()

                    if deletion_size > chunk_size:
                        list_above_chunk.append(replica)
                    else:
                        list_chunk.append(replica)

                    deletion_size += size

                list_chunk.extend(list_above_chunk)

                # Do a last-minute check whether we can really delete these replicas
                # Replicas that shouldn't be deleted are removed from list_chunk
                # Decision here is not recorded in the replica snapshots - should really
                # be an emergency measure
                if policy.predelete_check is not None:
                    policy.predelete_check(list_chunk)
                
                chunk_record = self.transaction_manager.deletion.schedule_deletions(list_chunk, comments = comment, is_test = is_test)
                if is_test:
                    # record deletion_id always starts from -1 and go negative
                    for deletion_id, record in chunk_record.iteritems():
                        while deletion_id in deletion_mapping:
                            deletion_id -= 1
                        deletion_mapping[deletion_id] = record
                else:
                    deletion_mapping.update(chunk_record)

            total_size = 0
            num_deleted = 0

            for deletion_id, (approved, replicas) in deletion_mapping.iteritems():
                size = sum([r.size() for r in replicas])

                if approved and not is_test:
                    for replica in replicas:
                        self.inventory_manager.store.delete_blockreplicas(replica.block_replicas)
                        deleted_blocks = set(br.block for br in replica.block_replicas)
                        dataset_blocks = set(replica.dataset.blocks)
                        if deleted_blocks == dataset_blocks:
                            # this replica was completely deleted
                            # second arg is False because block replicas must be all gone by now
                            self.inventory_manager.store.delete_datasetreplica(replica, delete_blockreplicas = False)

                    total_size += size
                    num_deleted += len(replicas)

                else:
                    # restore dataset-replica and site-replica links
                    for replica in replicas:
                        replica.link()

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            sigint.unblock()

            logger.info('Done deleting %d replicas (%.1f TB) from %s.', num_deleted, total_size * 1.e-12, site.name)
