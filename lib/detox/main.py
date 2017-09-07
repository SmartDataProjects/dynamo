import time
import logging
import math
import pprint
import collections
import random
import sys
import os

import common.configuration as config
from common.dataformat import Dataset, Block, Site, DatasetReplica, BlockReplica
from policy import Dismiss, Delete, DeleteOwner, Keep, Protect, Policy
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

#        Activity lock file is now written by dynamod
#        if not config.read_only and not is_test:
#            # write a file indicating detox activity
#            while True:
#                if os.path.exists(detox_config.main.activity_indicator):
#                    logger.info('Detox activity indicator exists. Waiting for 60 seconds.')
#                    time.sleep(60)
#                else:
#                    break
#
#            with open(detox_config.main.activity_indicator, 'w') as indicator:
#                indicator.write('Detox started: ' + time.strftime('%Y-%m-%d %H:%M:%S') + '\n')

        # Execute the policy within a try block to avoid dead locks
        try:
            # insert new policy lines to the history database
            logger.info('Saving policy conditions.')
            self.history.save_conditions(policy.rules)
    
            logger.info('Updating dataset demands.')
            # update requests, popularity, and locks
            self.demand_manager.update(self.inventory_manager, policy.used_demand_plugins)
    
            logger.info('Updating site status.')
            # update site status
            self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates
    
            self._execute_policy(policy, is_test, comment)

        finally:
            pass
#            if not config.read_only and not is_test and os.path.exists(detox_config.main.activity_indicator):
#                os.remove(detox_config.main.activity_indicator)
#        Activity lock file is now written by dynamod

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def _execute_policy(self, policy, is_test, comment):
        # if no policy line requires iterative execution, we need the sites to have non-negative quotas
        if not policy.static_optimization:
            for site in self.inventory_manager.sites.itervalues():
                if site.partition_quota(policy.partition) < 0.: # the site has infinite quota
                    logger.error('Finite quota for all sites is required for partition %s.', policy.partition.name)
                    return

        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        logger.info('Preparing deletion run %d', run_number)

        logger.info('Identifying target sites.')

        # Ask each site if deletion should be triggered.
        delete_target_sites = set()
        dismiss_target_sites = set()
        for site in self.inventory_manager.sites.itervalues():
            if policy.target_site_def.match(site):
                delete_target_sites.add(site)
                if policy.deletion_trigger.match(site):
                    dismiss_target_sites.add(site)

        logger.info('Identifying dataset replicas in the partition.')

        # "partition" as a verb - selecting only the blockreps in the partition
        # will also select out replicas on sites with quotas
        all_replicas = policy.partition_replicas(self.inventory_manager, delete_target_sites)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, delete_target_sites)
        self.history.save_datasets(run_number, set(r.dataset for r in all_replicas))
        # take snapshots of quotas if updated
        quotas = dict((site, site.partition_quota(policy.partition)) for site in self.inventory_manager.sites.itervalues())
        self.history.save_quotas(run_number, quotas)

        logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(all_replicas))

        protected = {} # {replica: condition_id or ([block_replica], condition_id)}
        deleted = {}
        kept = {}

        protected_fraction = collections.defaultdict(float) # {site: protected size}

        def update_protected_fraction(site, size):
            quota = site.partition_quota(policy.partition) * 1.e+12
            if quota > 0.:
                protected_fraction[site] += size / quota
            else:
                protected_fraction[site] = 0.

        def apply_protect(replica, condition):
            all_replicas.remove(replica)

            # we have a dataset-level protection
            # revert whatever we have done at block level

            if replica in deleted: # if the replica had matched DeleteBlock
                block_replicas, condition = deleted.pop(replica)
                # revert what is done under DeleteBlock
                replica.block_replicas.extend(block_replicas)
                for block_replica in block_replicas:
                    dataset_replica.site.add_block_replica(block_replica)

            if replica in protected:
                block_replicas, condition = protected.pop(replica)
                replica.block_replicas.extend(block_replicas)

            protected[replica] = condition

            return replica.size()

        def apply_delete(replica, condition):
            all_replicas.remove(replica)

            do_delete = (replica.site in delete_target_sites)

            if replica in protected: # if the replica had matched ProtectBlock
                protected_blocks = protected[replica][0]
                blocks_to_delete = list(set(replica.block_replicas) - set(protected_blocks))

                if do_delete:
                    return apply_deleteblock(replica, blocks_to_delete, condition)
                else:
                    kept[replica] = (blocks_to_delete, condition)
                    return 0

            else:
                if do_delete:
                    self.inventory_manager.unlink_datasetreplica(replica)
                    deleted[replica] = condition
                    return replica.size()
                else:
                    kept[replica] = condition
                    return 0

        def apply_deleteowner(replica, groups, condition):
            # This is a rather specific operation. The assumptions are that
            #  . owner groups that are targeted have block-level ownership (e.g. DataOps)
            #  . there may be a block that is owned by a group that has dataset-level ownership (e.g. AnalysisOps)

            dr_owner = None
            matching_brs = []
            for block_replica in replica.block_replicas:
                if block_replica.group.olevel is Dataset and dr_owner is None:
                    # there is a dataset-level owner
                    dr_owner = block_replica.group

                if block_replica.group in groups:
                    matching_brs.append(block_replica)

            if len(matching_brs) != 0:
                # act only when there is a block replica to do something on
                if len(matching_brs) == len(replica.block_replicas):
                    # all blocks matched - not reassigning to any group but deleting
                    return apply_delete(replica, condition)

                elif dr_owner is None:
                    # block replicas are marked for deletion, but we do not have a group that can take over
                    return apply_deleteblock(replica, matching_brs, condition)

                else:
                    # dr_owner is taking over
                    # not ideal to make reassignments here, but this operation affects later iterations
                    # not popping from all_replicas because different rules may now apply
                    self.reassign_owner(replica, matching_brs, dr_owner, policy.partition, is_test)
                    return 0

        def apply_protectblock(replica, block_replicas, condition):
            for block_replica in block_replicas:
                replica.block_replicas.remove(block_replica)

            protected[replica] = (block_replicas, condition)

            if len(replica.block_replicas) == 0:
                # take this out of policy evaluation for the next round
                all_replicas.remove(replica)

            return sum(br.size for br in block_replicas)

        def apply_deleteblock(replica, block_replicas, condititon):
            site = replica.site
            do_delete = (site in delete_target_sites)

            for block_replica in block_replicas:
                replica.block_replicas.remove(block_replica)
                if do_delete:
                    site.remove_block_replica(block_replica)

            empty = (len(replica.block_replicas) == 0)

            if empty:
                # take this out of policy evaluation for the next round
                all_replicas.remove(replica)
           
            if do_delete:
                if empty:
                    self.inventory_manager.unlink_datasetreplica(replica)

                deleted[replica] = (block_replicas, condition)
                return sum(br.size for br in block_replicas)
            else:
                return 0


        iteration = 0

        # now iterate through deletions, updating site usage as we go
        # if static_optimization is True, break after first pass
        while True:
            if not policy.static_optimization:
                iteration += 1
                logger.info('Iteration %d', iteration)

            # call policy.evaluate for each replica
            # parallel_exec is just a speed optimization (may not be meaningful in the presence of python Global Interpreter Lock)
            eval_results = parallel_exec(policy.evaluate, list(all_replicas), per_thread = 100)

            deletion_candidates = collections.defaultdict(dict) # {site: {replica: condition_id or ([block_replica], condition_id)}}

            iter_keep = {}

            # sort the evaluation results into protected, deleted, owner-deleted, and deletion_candidates
            for replica, action, condition in eval_results:
                if isinstance(action, Protect):
                    size = apply_protect(replica, condition)
                    if not policy.static_optimization:
                        update_protected_fraction(replica.site, size)

                elif isinstance(action, Delete):
                    apply_delete(replica, condition)

                elif isinstance(action, DeleteOwner):
                    apply_deleteowner(replica, action.groups, condition)

                elif isinstance(action, Dismiss):
                    if replica.site in dismiss_target_sites:
                        deletion_candidates[replica.site][replica] = condition
                    else:
                        iter_keep[replica] = condition

                elif isinstance(action, ProtectBlock):
                    size = apply_protectblock(replica, action.block_replicas, condition)
                    if not policy.static_optimization:
                        update_protected_fraction(replica.site, size)

                elif isinstance(action, DeleteBlock):
                    apply_deleteblock(replica, action.block_replicas, condition)

            logger.info(' %d dataset replicas in deletion candidates', sum(len(d) for d in deletion_candidates.itervalues()))
            logger.info(' %d dataset replicas in protection list', len(protected))

            if len(iter_keep) == len(all_replicas):
                # no more deletion candidates
                kept.update(iter_keep)
                break

            # now move deletion candidates to deleted until site hits the stop condition
            # first determine which sites to process

            if policy.static_optimization:
                target_sites = deletion_candidates.keys()
            else:
                # iterative deletion happens at one site at a time
                candidate_sites = deletion_candidates.keys()
        
                if len(protected) != 0:
                    # find the site with the highest protected fraction
                    target_site = max(candidate_sites, key = lambda site: protected_fraction[site])
                else:
                    target_site = random.choice(candidate_sites)

                target_sites = [target_site]

            for site in target_sites:
                site_candidates = deletion_candidates[site]

                # sort the candidates within the site
                sorted_candidates = sorted(site_candidates.iterkeys(), key = policy.candidate_sort_key)
    
                deleted_volume = 0.
    
                quota = site.partition_quota(policy.partition) * 1.e+12
    
                for replica in sorted_candidates:
                    if policy.stop_condition.match(site):
                        break
    
                    if logger.getEffectiveLevel() == logging.DEBUG:
                        logger.debug('Deleting replica: %s', str(replica))

                    data = site_candidates[replica]

                    if type(data) is tuple:
                        size = apply_deleteblock(replica, data[0], data[1])
                    else:
                        size = apply_delete(replica, data)
    
                    if not policy.static_optimization and quota > 0.:
                        deleted_volume += size
                        if deleted_volume / quota > detox_config.main.deletion_per_iteration:
                            break

            if policy.static_optimization:
                # we are done
                kept.update(iter_keep)
                break

            # update the list of target sites
            for site in list(dismiss_target_sites):
                if policy.stop_condition.match(site):
                    dismiss_target_sites.remove(site)

        # done iterating

        for rule in policy.rules:
            if hasattr(rule, 'has_match') and not rule.has_match:
                logger.warning('Policy %s had no matching replica.' % str(rule))

        # save replica snapshots and all deletion decisions
        logger.info('Saving deletion decisions.')

        self.history.save_deletion_decisions(run_number, deleted, kept, protected)
        
        logger.info('Committing deletion.')
        self.commit_deletions(run_number, policy, deleted, is_test, comment)

        logger.info('Restoring inventory state.')

        # first recover fragmented dataset replicas
        for replica, data in protected.iteritems():
            if type(data) is tuple:
                replica.block_replicas.extend(data[0])

        for replica, data in deleted.iteritems():
            if type(data) is tuple:
                replica.block_replicas.extend(data[0])

        # then bring back replicas not in the partition
        policy.restore_replicas()

        self.history.close_deletion_run(run_number)

    def reassign_owner(self, dataset_replica, block_replicas, new_owner, partition, is_test):
        self.transaction_manager.copy.schedule_reassignments(block_replicas, new_owner, comments = 'Dynamo -- Group reassignment', is_test = is_test)

        site = dataset_replica.site

        new_replicas = []
        for old_replica in block_replicas:
            dataset_replica.block_replicas.remove(old_replica)
            site.remove_block_replica(old_replica)

            new_replica = old_replica.clone(group = new_owner)

            dataset_replica.block_replicas.append(new_replica)
            site.add_block_replica(new_replica, partitions = [partition])
            
            new_replicas.append(new_replica)

        if not is_test:
            self.inventory_manager.store.add_blockreplicas(new_replicas)

    def commit_deletions(self, run_number, policy, deletion_list, is_test, comment):
        """
        @param run_number    Cycle number.
        @param policy        Policy object.
        @param deletion_list {replica: condition_id or ([block_replica], condition_id)}
        @param is_test       Do not actually delete if True
        @param comment       Comment to be recorded in the history DB
        """

        if not comment:
            comment = 'Dynamo -- Automatic cache release request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        # organize the replicas into sites and set up block-level deletions
        deletions_by_site = collections.defaultdict(list)
        original_blocks = []
        for replica, data in deletion_list.iteritems():
            if type(data) is tuple: # block delete - data[0] is a list of blockreplicas
                original_blocks.append((replica, replica.block_replicas))
                replica.block_replicas = data[0]

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
                        replica_list.extend(list_above_chunk)
                        list_above_chunk = []
                        break

                    deletion = site_deletion_list.pop()

                    if deletion_size > chunk_size:
                        list_above_chunk.append(deletion)
                    else:
                        list_chunk.append(deletion)

                    deletion_size += size

                list_chunk.extend(list_above_chunk)
                
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
                if approved and not is_test:
                    for replica in replicas:
                        self.inventory_manager.store.delete_blockreplicas(replica.block_replicas)
                        deleted_blocks = set(br.block for br in replica.block_replicas)
                        dataset_blocks = set(replica.dataset.blocks)
                        if deleted_blocks == dataset_blocks:
                            # this replica was completely deleted
                            # second arg is False because block replicas must be all gone by now
                            self.inventory_manager.store.delete_datasetreplica(replica, delete_blockreplicas = False)
                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)
                total_size += size
                num_deleted += len(replicas)

            sigint.unblock()

            logger.info('Done deleting %d replicas (%.1f TB) from %s.', num_deleted, total_size * 1.e-12, site.name)

        # bring block-deleted replicas back to original state
        for replica, block_replicas in original_blocks:
            replica.block_replicas = block_replicas
