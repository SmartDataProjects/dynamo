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

    def run(self, policy, is_test = False, comment = '', auto_approval = True):
        """
        Main executable.

        @param policy     A Detox Policy object
        @param is_test    Set to True when e.g. the main binary is invoked with --test-run option.
        @param comment    Passed to dynamo history as well as the deletion interface
        @param auto_approval
        """

        logger.info('Detox cycle for %s starting at %s', policy.partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

        if not config.read_only and not is_test:
            # write a file indicating detox activity
            while True:
                if os.path.exists(detox_config.activity_indicator):
                    logger.info('Detox activity indicator exists. Waiting for 60 seconds.')
                    time.sleep(60)
                else:
                    break

            with open(detox_config.activity_indicator, 'w') as indicator:
                indicator.write('Detox started: ' + time.strftime('%Y-%m-%d %H:%M:%S') + '\n')

        # Execute the policy within a try block to avoid dead locks
        try:
            self._execute_policy(policy, is_test, comment, auto_approval)

        finally:
            if not config.read_only and not is_test and os.path.exists(detox_config.activity_indicator):
                os.remove(detox_config.activity_indicator)

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def _execute_policy(self, policy, is_test, comment, auto_approval):
        # if no policy line requires iterative execution, we need the sites to have non-negative quotas
        if not policy.static_optimization:
            for site in self.inventory_manager.sites.values():
                if site.partition_quota(policy.partition) < 0.: # the site has infinite quota
                    logger.error('Finite quota for all sites is required for partition %s.', policy.partition.name)
                    return

        # insert new policy lines to the history database
        self.history.save_conditions(policy.rules)

        # update requests, popularity, and locks
        self.demand_manager.update(self.inventory_manager, policy.used_demand_plugins)

        # update site status
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(policy.partition.name, policy.version, is_test = is_test, comment = comment)

        logger.info('Preparing deletion run %d', run_number)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, self.inventory_manager)
        self.history.save_datasets(run_number, self.inventory_manager)
        # take snapshots of quotas if updated
        quotas = dict((site, site.partition_quota(policy.partition)) for site in self.inventory_manager.sites.values())
        self.history.save_quotas(run_number, quotas)

        logger.info('Identifying target sites.')

        # Ask each site if deletion should be triggered.
        target_sites = set()
        for site in self.inventory_manager.sites.values():
            if site.partition_quota(policy.partition) != 0. and policy.target_site_def.match(site) and policy.deletion_trigger.match(site):
                target_sites.add(site)

        logger.info('Identifying dataset replicas in the partition.')

        # "partition" as a verb - selecting only the blockreps in the partition
        # will also select out replicas on sites with quotas
        all_replicas = policy.partition_replicas(self.inventory_manager.datasets.values())

        logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(all_replicas))

        protected = {} # {replica: condition_id}
        deleted = {}

        protected_fraction = collections.defaultdict(float) # {site: protected size}

        iteration = 0

        # now iterate through deletions, updating site usage as we go
        # if static_optimization is True, break after first pass
        while True:
            if not policy.static_optimization:
                iteration += 1
                logger.info('Iteration %d', iteration)

            # call policy.evaluate for each replica
            # parallel_exec is just a speed optimization (may not be meaningful in the presence of python Global Interpreter Lock)
            eval_results = parallel_exec(lambda r: policy.evaluate(r), list(all_replicas), per_thread = 100)

            deletion_candidates = collections.defaultdict(dict) # {site: {replica: condition_id}}

            # sort the evaluation results into protected, deleted, owner-deleted, and deletion_candidates
            for replica, decision, condition in eval_results:
                if isinstance(decision, Protect):
                    all_replicas.remove(replica)
                    protected[replica] = condition
                    if not policy.static_optimization:
                        protected_fraction[replica.site] += replica.size() / replica.site.partition_quota(policy.partition)

                elif isinstance(decision, Delete):
                    self.inventory_manager.unlink_datasetreplica(replica)
                    all_replicas.remove(replica)
                    deleted[replica] = condition

                elif isinstance(decision, DeleteOwner):
                    # This is a rather specific operation. The assumptions are that
                    #  . owner groups that are targeted have block-level ownership (e.g. DataOps)
                    #  . there may be a block that is owned by a group that has dataset-level ownership (e.g. AnalysisOps)

                    dr_owner = None
                    matching_brs = []
                    for block_replica in replica.block_replicas:
                        if block_replica.group.olevel is Dataset and dr_owner is None:
                            # there is a dataset-level owner
                            dr_owner = block_replica.group

                        if block_replica.group in decision.groups:
                            matching_brs.append(block_replica)

                    if len(matching_brs) != 0:
                        # act only when there is a block replica to do something on

                        if len(matching_brs) == len(replica.block_replicas):
                            # all blocks matched - not reassigning to any group but deleting
                            self.inventory_manager.unlink_datasetreplica(replica)
                            all_replicas.remove(replica)
                            deleted[replica] = condition

                        elif dr_owner is None:
                            # block replicas are marked for deletion, but we do not have a group that can take over
                            # -> pass until block-level deletion is implemented
                            pass
                        else:
                            # dr_owner is taking over
                            # not ideal to make reassignments here, but this operation affects later iterations
                            # not popping from all_replicas because different rules may now apply
                            self.reassign_owner(replica, matching_brs, dr_owner, policy.partition, is_test)

                elif replica.site in target_sites:
                    deletion_candidates[replica.site][replica] = condition

            logger.info(' %d dataset replicas in deletion candidates', sum(len(d) for d in deletion_candidates.values()))
            logger.info(' %d dataset replicas in protection list', len(protected))

            if len(deletion_candidates) == 0:
                # no more deletion candidates
                break

            if policy.static_optimization:
                # first pass done under static optimization
                # add all deletion candidates to deleted and break
                deleted.update(self.determine_deletions(target_sites, deletion_candidates, policy))
                break

            # iterative deletion happens one site at a time
            candidate_sites = deletion_candidates.keys()
    
            if len(protected) != 0:
                # find the site with the highest protected fraction
                target_site = max(candidate_sites, key = lambda site: protected_fraction[site])
            else:
                target_site = random.choice(candidate_sites)

            iter_deletion = self.determine_deletions([target_site], deletion_candidates, policy)

            for replica in iter_deletion:
                all_replicas.remove(replica)

            deleted.update(iter_deletion)

            # update the list of target sites
            for site in list(target_sites):
                if policy.stop_condition.match(site):
                    target_sites.remove(site)

        kept = {}
        # remaining replicas not in protected or deleted are kept
        for replica, decision, condition in eval_results:
            if not isinstance(decision, Protect) and replica not in deleted:
                kept[replica] = condition

        for rule in policy.rules:
            if hasattr(rule, 'has_match') and not rule.has_match:
                logger.warning('Policy %s had no matching replica.' % str(rule))

        # save replica snapshots and all deletion decisions
        logger.info('Saving deletion decisions.')

        self.history.save_deletion_decisions(run_number, deleted, kept, protected)
        
        logger.info('Committing deletion.')
        self.commit_deletions(run_number, policy, deleted.keys(), is_test, comment, auto_approval)

        logger.info('Restoring inventory state.')
        policy.restore_replicas()

        self.history.close_deletion_run(run_number)

    def determine_deletions(self, target_sites, deletion_candidates, policy):
        """
        Order the deletion candidates at each site according to the policy candidate_sort
        and report back the replicas to be deleted. Will only request deletion of volume
        up to detox_config.deletion_per_iteration.

        @param target_sites        Sites to report deletions on.
        @param deletion_candidates {site: {replica: condition}}
        @param policy              Policy

        @return A dict {replica: condition}
        """

        deleted = {}

        for site in target_sites:
            if site not in deletion_candidates:
                continue

            site_candidates = deletion_candidates[site]

            sorted_candidates = policy.candidate_sort(site_candidates.keys())

            deleted_volume = 0.

            quota = site.partition_quota(policy.partition)

            for replica in sorted_candidates:
                if policy.stop_condition.match(site):
                    break

                if logger.getEffectiveLevel() == logging.DEBUG:
                    logger.debug('Deleting replica: %s', str(replica))

                # take out replicas from inventory and from the list of considered replicas
                self.inventory_manager.unlink_datasetreplica(replica)

                deleted[replica] = site_candidates[replica]
                
                if not policy.static_optimization:
                    deleted_volume += replica.size() * 1.e-12
                    if deleted_volume / quota > detox_config.deletion_per_iteration:
                        break

        return deleted

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

    def commit_deletions(self, run_number, policy, deletion_list, is_test, comment, auto_approval):
        sites = set(r.site for r in deletion_list)

        if not comment:
            comment = 'Dynamo -- Automatic cache release request'
            if policy.partition.name != 'Global':
                comment += ' for %s partition.' % policy.partition.name

        # now schedule deletion for each site
        for site in sorted(sites):
            if site.storage_type == Site.TYPE_MSS:
                if config.daemon_mode:
                    logger.warning('Deletion from MSS cannot be done in daemon mode.')
                    continue
            
                print 'Deletion from', site.name, 'is requested. Are you sure? [Y/n]'
                response = sys.stdin.readline().strip()
                if response != 'Y':
                    logger.warning('Aborting.')
                    continue

            replica_list = [r for r in deletion_list if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            sigint.block()

            deletion_mapping = {} #{deletion_id: (approved, [replicas])}

            chunk_size = detox_config.deletion_volume_per_request * 1.e+12

            while len(replica_list) != 0:
                # stack up replicas up to 110% of volume_per_request
                # unnecessary complication in my mind, but has been requested
                list_chunk = []
                list_above_chunk = []
                deletion_size = 0
                while len(replica_list) != 0:
                    size = replica_list[-1].size()
                    if deletion_size > chunk_size and deletion_size + size > chunk_size * 1.1:
                        # put the excess back
                        list_above_chunk.reverse()
                        replica_list.extend(list_above_chunk)
                        list_above_chunk = []
                        break

                    replica = replica_list.pop()

                    if deletion_size > chunk_size:
                        list_above_chunk.append(replica)
                    else:
                        list_chunk.append(replica)

                    deletion_size += size

                list_chunk.extend(list_above_chunk)
                
                chunk_record = self.transaction_manager.deletion.schedule_deletions(list_chunk, comments = comment, auto_approval = auto_approval, is_test = is_test)
                if is_test:
                    # record deletion_id always starts from -1 and go negative
                    for deletion_id, record in chunk_record.items():
                        while deletion_id in deletion_mapping:
                            deletion_id -= 1
                        deletion_mapping[deletion_id] = record
                else:
                    deletion_mapping.update(chunk_record)

            total_size = 0
            num_deleted = 0

            for deletion_id, (approved, replicas) in deletion_mapping.items():
                if approved and not is_test:
                    for replica in replicas:
                        self.inventory_manager.store.delete_blockreplicas(replica.block_replicas)
                        if replica not in policy.untracked_replicas:
                            # this replica was fully in the partition
                            # second arg is False because block replicas must be all gone by now
                            self.inventory_manager.store.delete_datasetreplica(replica, delete_blockreplicas = False)
                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)
                total_size += size
                num_deleted += len(replicas)

            sigint.unblock()

            logger.info('Done deleting %d replicas (%.1f TB) from %s.', num_deleted, total_size * 1.e-12, site.name)
