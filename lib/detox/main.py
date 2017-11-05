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
            for targdef in policy.target_site_def:
                if targdef.match(site):
                    target_sites.add(site)
                    break
            else:
                # not a target site
                continue

            for trigger in policy.deletion_trigger:
                if trigger.match(site):
                    triggered_sites.add(site)
                    break

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

        protected_fraction = dict((s, 1. if q == 0 else 0.) for s, q in quotas.iteritems())

        protected = collections.defaultdict(list) # {replica: [([block_replica], condition_id)]}
        deleted = collections.defaultdict(list) # same
        kept = collections.defaultdict(list) # same

        iteration = 0

        # now iterate through deletions, updating site usage as we go
        while True:
            iteration += 1
            logger.info('Iteration %d, evaluating %d replicas', iteration, len(all_replicas))

            delete_candidates = collections.defaultdict(list) # {replica: [([block_replica], condition_id)]}
            keep_candidates = collections.defaultdict(list) # {replica: [([block_replica], condition_id)]}
            protect_candidates = collections.defaultdict(list) # {replica: [([block_replica], condition_id)]}

            # Call policy.evaluate for each replica
            # Function evaluate() returns a list of actions. If the replica matches a dataset-level policy, there is only one element in the returned list.
            # Sort the evaluation results into containers
            # Block-level actions are triggered only if the condition does not apply to all blocks
            # Policy object issues a dataset-level action otherwise
            empty_replicas = []
            start = time.time()
            for replica in all_replicas:
                actions = policy.evaluate(replica)

                # Block-level actions come first - take out all blocks that matched some condition.
                # Remaining block replicas are the ones the dataset-level action applies to.
                block_replicas = set(replica.block_replicas)

                for action in actions:
                    matched_line = action.matched_line
                    if matched_line is None:
                        condition_id = 0
                    else:
                        condition_id = matched_line.condition_id

                    if isinstance(action, ProtectBlock):
                        protect_candidates[replica].append((action.block_replicas, condition_id))

                        block_replicas -= set(action.block_replicas)
    
                    elif isinstance(action, DeleteBlock):
                        unlinked_replicas = self.unlink_block_replicas(replica, action.block_replicas, policy, is_test)
                        if len(unlinked_replicas) != 0:
                            deleted[replica].append((unlinked_replicas, condition_id))

                            block_replicas -= set(unlinked_replicas)

                    elif isinstance(action, DismissBlock):
                        if replica.site in triggered_sites:
                            delete_candidates[replica].append((action.block_replicas, condition_id))
                        else:
                            keep_candidates[replica].append((action.block_replicas, condition_id))

                        block_replicas -= set(action.block_replicas)
    
                    elif isinstance(action, Protect):
                        protect_candidates[replica].append((list(block_replicas), condition_id))
    
                    elif isinstance(action, Delete):
                        unlinked_replicas = self.unlink_block_replicas(replica, block_replicas, policy, is_test)
                        if len(unlinked_replicas) != 0:
                            deleted[replica].append((unlinked_replicas, condition_id))

                        if len(replica.block_replicas) == 0:
                            # if all blocks were deleted, take the replica off all_replicas for later iterations
                            # this is the only place where the replica can become empty
                            empty_replicas.append(replica)

                        # no need to update block_replicas set with reassigned blockreplicas because we don't
                        # need it any more
    
                    elif isinstance(action, Dismiss):
                        if replica.site in triggered_sites:
                            delete_candidates[replica].append((list(block_replicas), condition_id))
                        else:
                            keep_candidates[replica].append((list(block_replicas), condition_id))

            for replica in empty_replicas:
                all_replicas.remove(replica)

            logger.info('Took %f seconds to evaluate', time.time() - start)

            logger.info(' %d dataset replicas in deletion candidates', len(delete_candidates))

            if len(delete_candidates) != 0:
                # now figure out which of deletion candidates to actually delete
                # first determine which sites to process
    
                # delete from one site at a time

                # compute the increment on the protected fractions
                fraction_increments = dict((site, 0.) for site in protected_fraction.iterkeys())
                for replicas, matches in protect_candidates.iteritems():
                    quota = quotas[replica.site] * 1.e+12
                    if quota > 0.:
                        size = sum(sum(br.size for br in match[0]) for match in matches)
                        fraction_increments[replica.site] += size / quota
       
                # find the site with the highest protected fraction
                candidate_sites = set(r.site for r in delete_candidates.iterkeys())
                selected_site = max(candidate_sites, key = lambda site: protected_fraction[site] + fraction_increments[site])

                candidates_at_site = [r for r in delete_candidates.iterkeys() if r.site == selected_site]
                replicas_to_delete = sorted(candidates_at_site, key = policy.candidate_sort_key)
    
                deleted_volume = collections.defaultdict(float)
    
                for replica in replicas_to_delete:
                    site = replica.site

                    # has the site reached the stop-deletion threshold?
                    offtrigger = False
                    for cond in policy.stop_condition:
                        if cond.match(site):
                            offtrigger = True
                            break

                    if offtrigger:
                        continue
    
                    quota = quotas[site] * 1.e+12

                    # have we deleted more than allowed in a single iteration?
                    if quota > 0. and deleted_volume[site] / quota > detox_config.main.deletion_per_iteration:
                        continue
    
                    if logger.getEffectiveLevel() == logging.DEBUG:
                        logger.debug('Deleting replica: %s', str(replica))
    
                    matches = delete_candidates.pop(replica)
    
                    for match in matches:
                        # match = ([block_replica], condition_id)
                        unlinked_replicas = self.unlink_block_replicas(replica, match[0], policy, is_test)
                        if len(unlinked_replicas) != 0:
                            deleted_volume[site] += sum(br.size for br in unlinked_replicas)
                            deleted[replica].append((unlinked_replicas, match[1]))

                    if len(replica.block_replicas) == 0:
                        all_replicas.remove(replica)

            # remaining delete_candidates are kept
            for replica, matches in delete_candidates.iteritems():
                keep_candidates[replica].extend(matches)

            if len(delete_candidates) == 0:
                # we are done
                for replica, matches in protect_candidates.iteritems():
                    protected[replica].extend(matches)

                for replica, matches in keep_candidates.iteritems():
                    kept[replica].extend(matches)

                break

            else:
                # commit protected candidates if not in keep list
                # otherwise these replicas are re-evaluated
                for replica, matches in protect_candidates.iteritems():
                    if replica in keep_candidates:
                        continue

                    protected[replica].extend(matches)
                    all_replicas.remove(replica)

                    quota = quotas[replica.site] * 1.e+12
                    if quota > 0.:
                        size = sum(sum(br.size for br in match[0]) for match in matches)
                        protected_fraction[replica.site] += size / quota
    
                # update the list of target sites
                for site in list(triggered_sites):
                    for cond in policy.stop_condition:
                        if cond.match(site):
                            triggered_sites.remove(site)
                            break

        # done iterating

        logger.info(' %d dataset replicas in delete list', len(deleted))
        logger.info(' %d dataset replicas in keep list', len(kept))
        logger.info(' %d dataset replicas in protect list', len(protected))

        for line in policy.policy_lines:
            if hasattr(line, 'has_match') and not line.has_match:
                logger.warning('Policy %s had no matching replica.' % str(line))

        # save replica snapshots and all deletion decisions
        logger.info('Saving deletion decisions.')

        self.history.save_deletion_decisions(run_number, quotas, deleted, kept, protected)
        
        logger.info('Committing deletion.')

        # we have recorded deletion reasons; we can now consolidate deleted block replicas

        # put aside the block replicas to not delete
        keep_parts = {}

        deletion_list = []
        for replica, matches in deleted.iteritems():
            keep_parts[replica] = replica.block_replicas
            replica.block_replicas = []
            for match in matches:
                replica.block_replicas.extend(match[0])

            deletion_list.append(replica)

        self.commit_deletions(run_number, policy, deletion_list, is_test, comment)

        logger.info('Restoring inventory state.')

        # recover fragmented dataset replicas
        for replica, block_replicas in keep_parts.iteritems():
            replica.block_replicas.extend(block_replicas)

        # then bring back replicas not in the partition
        policy.restore_replicas()

        self.history.close_deletion_run(run_number)

    def unlink_block_replicas(self, replica, block_replicas, policy, is_test):
        """
        Unlink the dataset replica or parts of it from the owning containers.
        Return the list of unlinked block replicas and reowned block replicas.
        The second list is necessary for the caller to update its list of block
        replicas to process, because owner change amounts to a rewrite of the
        entire object under the current immutable blockreplica format.
        """

        if len(block_replicas) == len(replica.block_replicas):
            replica.dataset.remove(replica)
            replica.site.remove_dataset_replica(replica)

            blocks_to_unlink = list(block_replicas)

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
                blocks_to_hand_over = []
                blocks_to_unlink = list(block_replicas)
            else:
                blocks_to_hand_over = []
                blocks_to_unlink = []
                for block_replica in block_replicas:
                    if block_replica.group.olevel is Dataset:
                        blocks_to_unlink.append(block_replica)
                    else:
                        blocks_to_hand_over.append(block_replica)

            if len(blocks_to_hand_over) != 0:
                logger.debug('%d blocks to hand over to %s', len(blocks_to_hand_over), dr_owner.name)
                # not ideal to make reassignments here, but this operation affects later iterations
                self.reassign_owner(replica, blocks_to_hand_over, dr_owner, policy.partition, is_test)

            if len(blocks_to_unlink) != 0:
                logger.debug('%d blocks to unlink', len(blocks_to_unlink))

                for block_replica in blocks_to_unlink:
                    replica.block_replicas.remove(block_replica)

                replica.site.update_partitioning(replica)

        return blocks_to_unlink

    def reassign_owner(self, dataset_replica, block_replicas, new_owner, partition, is_test):
        """
        Add back the block replicas to dataset replica under the new owner.
        """

        self.transaction_manager.copy.schedule_reassignments(block_replicas, new_owner, comments = 'Dynamo -- Group reassignment', is_test = is_test)

        for replica in block_replicas:
            block_replica.group = new_owner

        if not is_test:
            # are we relying on do_update = True in insert_many <- add_blockreplicas here?
            self.inventory_manager.store.update_blockreplicas(block_replicas)

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
                for replica in replicas:
                    for block_replica in replica.block_replicas:
                        block_replica.group = None
                    
                    if not is_test:
                        self.inventory_manager.store.update_blockreplicas(replica.block_replicas)

                if approved and not is_test:
                    total_size += size
                    num_deleted += len(replicas)

                else:
                    # restore dataset-replica and site-replica links
                    # TODO IS THIS CORRECT?? Don't I double count block replicas??
                    for replica in replicas:
                        replica.dataset.replicas.append(replica)
                        replica.site.add_dataset_replica(replica)

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            sigint.unblock()

            logger.info('Done deleting %d replicas (%.1f TB) from %s.', num_deleted, total_size * 1.e-12, site.name)
