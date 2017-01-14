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
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

        self.policies = {}

    def set_policy(self, policy):
        """
        Can be called multiple times to set policies for different partitions.
        """

        self.policies[policy.partition] = policy

    def run(self, partition, is_test = False, comment = '', auto_approval = True):
        """
        Main executable.
        """

        logger.info('Detox cycle for %s starting at %s', partition.name, time.strftime('%Y-%m-%d %H:%M:%S'))

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

        try:
            policy = self.policies[partition]

            if not policy.static_optimization:
                for site in self.inventory_manager.sites.values():
                    if site.partition_quota(policy.partition) < 0.: # the site is active but does not have a quota
                        logger.error('Non-negative quota for all sites is required for partition %s.', partition.name)
                        return

            self.history.save_conditions(policy.rules)

            self.demand_manager.update(self.inventory_manager, accesses = policy.uses_accesses, requests = policy.uses_requests, locks = policy.uses_locks)
            self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates
    
            # fetch the copy/deletion run number
            run_number = self.history.new_deletion_run(partition.name, policy.version, is_test = is_test, comment = comment)
    
            logger.info('Preparing deletion run %d', run_number)
    
            # update site and dataset lists
            # take a snapshot of site status
            self.history.save_sites(run_number, self.inventory_manager)
            self.history.save_datasets(run_number, self.inventory_manager)
            # take snapshots of quotas if updated
            quotas = dict((site, site.partition_quota(partition)) for site in self.inventory_manager.sites.values())
            self.history.save_quotas(run_number, quotas)
    
            logger.info('Identifying target sites.')
    
            # Ask each site if deletion should be triggered.
            target_sites = set()
            for site in self.inventory_manager.sites.values():
                if site.partition_quota(partition) != 0. and policy.target_site_def.match(site) and policy.deletion_trigger.match(site):
                    target_sites.add(site)

#            logger.info('Target sites: %s', ' '.join([s.name for s in target_sites]))

            logger.info('Identifying dataset replicas in the partition.')

            # "partition" as a verb - selecting only the blockreps in the partition
            # will also select out replicas on sites with quotas
            all_replicas = policy.partition_replicas(self.inventory_manager.datasets.values())

            logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(all_replicas))
    
            protected = {} # {replica: condition_id}
            deleted = {}

            protected_fraction = collections.defaultdict(float) # {site: protected size}
    
            iteration = 0
    
            while True:
                if not policy.static_optimization:
                    iteration += 1
                    logger.info('Iteration %d', iteration)
    
                eval_results = parallel_exec(lambda r: policy.evaluate(r), list(all_replicas), per_thread = 100)

                deletion_candidates = collections.defaultdict(dict) # {site: {replica: condition_id}}

                for replica, decision, condition in eval_results:
                    if isinstance(decision, Protect):
                        all_replicas.remove(replica)
                        protected[replica] = condition
                        if not policy.static_optimization:
                            protected_fraction[replica.site] += replica.size() / replica.site.partition_quota(partition)

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
                    break

                if policy.static_optimization:
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
            deleted_replicas = self.commit_deletions(run_number, policy, deleted.keys(), is_test, comment, auto_approval)
    
            logger.info('Restoring inventory state.')
            policy.restore_replicas()
    
            logger.info('Removing datasets with no replicas.')
            datasets_to_remove = []
            for dataset in set([replica.dataset for replica in deleted_replicas]):
                if len(dataset.replicas) == 0:
                    if not is_test:
                        datasets_to_remove.append(self.inventory_manager.datasets.pop(dataset.name))
                        dataset.unlink()

            if len(datasets_to_remove) != 0:
                self.inventory_manager.store.delete_datasets(datasets_to_remove)
    
            self.history.close_deletion_run(run_number)

        finally:
            if not config.read_only and not is_test and os.path.exists(detox_config.activity_indicator):
                os.remove(detox_config.activity_indicator)

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_deletions(self, target_sites, deletion_candidates, policy):
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

        deleted_replicas = []

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

            chunk_size = detox_config.deletion_volume_per_request

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
                            deleted_replicas.append(replica)

                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)
                total_size += size
                num_deleted += len(replicas)

            sigint.unblock()

            logger.info('Done deleting %d replicas (%.1f TB) from %s.', num_deleted, total_size * 1.e-12, site.name)

        return deleted_replicas


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from detox.policies.policies import ActionList
    from detox.policies.site import BelongsTo

    parser = ArgumentParser(description = 'Use detox')

    parser.add_argument('replica', metavar = 'SITE:DATASET', help = 'Replica to delete.')
    parser.add_argument('--partition', '-p', metavar = 'PARTITION', dest = 'partition', default = 'AnalysisOps', help = 'Partition to delete from.')
    parser.add_argument('--dry-run', '-D', action = 'store_true', dest = 'dry_run',  help = 'Dry run (no write / delete at all)')
    parser.add_argument('--production-run', '-P', action = 'store_true', dest = 'production_run', help = 'This is not a test.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    site_pattern, sep, dataset_pattern = args.replica.partition(':')
    
    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')
    
    kwd = {}
    for cls in ['store', 'site_source', 'dataset_source', 'replica_source']:
        kwd[cls + '_cls'] = classes.default_interface[cls]
    
    inventory_manager = InventoryManager(**kwd)
    
    transaction_manager = TransactionManager()
    
    demand_manager = DemandManager()

    history = classes.default_interface['history']()

    detox = Detox(inventory_manager, transaction_manager, demand_manager, history)

    action_list = ActionList()
    action_list.add_action('Delete', site_pattern, dataset_pattern)

    partition = Site.partitions[args.partition]

    policy = Policy(Policy.DEC_PROTECT, [action_list], Policy.ST_GREEDY, partition = partition, replica_requirement = BelongsTo(group))

    detox.set_policy(policy)

    if args.dry_run:
        config.read_only = True

    detox.run(partition, is_test = not args.production_run)
