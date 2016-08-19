import time
import logging
import math
import pprint
import collections
import random
import sys
import os

import common.configuration as config
from common.dataformat import Site, DatasetReplica, BlockReplica
from policy import Policy
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

    def run(self, partition = '', is_test = False, comment = ''):
        """
        Main executable.
        """

        if partition:
            logger.info('Detox cycle for %s starting at %s', partition, time.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            logger.info('Detox cycle starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        if not config.read_only and not is_test:
            # write a file indicating detox activity
            while True:
                if os.path.exists(detox_config.activity_indicator):
                    logger.info('Detox activity indicator exists. Waiting for 60 seconds.')
                    time.sleep(60)
                else:
                    break

            with open(detox_config.activity_indicator, 'w'):
                pass

        try:
            policy = self.policies[partition]

            self.history.save_conditions(policy.rules)

            self.demand_manager.update(self.inventory_manager, accesses = policy.uses_accesses, requests = policy.uses_requests, locks = policy.uses_locks)
            self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates
    
            # fetch the copy/deletion run number
            run_number = self.history.new_deletion_run(partition, policy.version, is_test = is_test, comment = comment)
    
            logger.info('Preparing deletion run %d', run_number)
    
            # update site and dataset lists
            # take a snapshot of site status
            self.history.save_sites(run_number, self.inventory_manager)
            self.history.save_datasets(run_number, self.inventory_manager)
            # take snapshots of quotas if updated
            self.history.save_quotas(run_number, partition, policy.quotas, self.inventory_manager)
    
            logger.info('Identifying target sites.')
    
            # Ask each site if deletion should be triggered.
            # Sites not in READY state or in IGNORE activity state are hard-coded to not be considered.
            target_sites = set()
            for site in self.inventory_manager.sites.values():
                if site in policy.quotas and policy.quotas[site] != 0. and policy.target_site_def.match(site) and policy.deletion_trigger.match(site):
                    target_sites.add(site)

            logger.info('Identifying dataset replicas in the partition.')

            # "partition" as a verb - selecting only the blockreps in the partition
            # will also select out replicas on sites with quotas
            all_replicas = policy.partition_replicas(self.inventory_manager.datasets.values())

            # take a snapshot of current replicas
            self.history.save_replicas(run_number, list(all_replicas))
    
            logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(all_replicas))
    
            protected = {} # {replica: reason}
            deleted = {}

            protected_fraction = collections.defaultdict(float) # {site: protected size}
    
            iteration = 0
    
            while True:
                iteration += 1

                if not policy.static_optimization:
                    logger.info('Iteration %d', iteration)
    
                eval_results = parallel_exec(lambda r: policy.evaluate(r), list(all_replicas), per_thread = 100)
    
                deletion_candidates = collections.defaultdict(dict) # {site: {replica: reason}}
    
                for replica, decision, reason in eval_results:
                    if decision == Policy.DEC_PROTECT:
                        all_replicas.remove(replica)
                        protected[replica] = reason
                        protected_fraction[replica.site] += replica.size() / policy.quotas[replica.site]

                    elif decision == Policy.DEC_DELETE_UNCONDITIONAL:
                        self.inventory_manager.unlink_datasetreplica(replica)
                        all_replicas.remove(replica)
                        deleted[replica] = reason
    
                    elif replica.site in target_sites:
                        deletion_candidates[replica.site][replica] = reason
    
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
            for replica, decision, reason in eval_results:
                if decision != Policy.DEC_PROTECT and replica not in deleted:
                    kept[replica] = reason
    
            for rule in policy.rules:
                if hasattr(rule, 'has_match') and not rule.has_match:
                    logger.warning('Policy %s had no matching replica.' % str(rule))
    
            # save replica snapshots and all deletion decisions
            logger.info('Saving deletion decisions.')
            self.history.save_deletion_decisions(run_number, protected, deleted, kept)
    
            logger.info('Committing deletion.')
            self.commit_deletions(run_number, policy, deleted.keys(), is_test, comment)
    
            logger.info('Restoring inventory state.')
            policy.restore_replicas()
    
            # remove datasets that lost replicas
            for dataset in set([replica.dataset for replica in deleted.keys()]):
                if len(dataset.replicas) == 0:
                    if not is_test:
                        self.inventory_manager.store.delete_dataset(dataset)
                        self.inventory_manager.datasets.pop(dataset.name)
    
                    dataset.unlink()
    
            self.history.close_deletion_run(run_number)

        finally:
            if not config.read_only and not is_test and os.path.exists(detox_config.activity_indicator):
                os.remove(detox_config.activity_indicator)

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def determine_deletions(self, target_sites, deletion_candidates, policy):
        for site in target_sites:
            if site not in deletion_candidates:
                continue

            site_candidates = deletion_candidates[site]

            sorted_candidates = policy.candidate_sort(site_candidates.keys())

            deleted = {}
            deleted_volume = 0.

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
                    if deleted_volume / policy.quotas[site] > detox_config.deletion_per_iteration:
                        break

            return deleted

    def commit_deletions(self, run_number, policy, deletion_list, is_test, comment):
        sites = set(r.site for r in deletion_list)

        if not comment:
            comment = 'Dynamo -- Automatic cache release request'
            if policy.partition:
                comment += ' for %s partition.' % policy.partition

        # now schedule deletion for each site
        for site in sorted(sites):
            replica_list = [r for r in deletion_list if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            sigint.block()

            deletion_mapping = {} #{deletion_id: (approved, [replicas])}

            while len(replica_list) != 0:
                list_chunk = []
                deletion_size = 0
                while deletion_size * 1.e-12 < detox_config.deletion_volume_per_request:
                    try:
                        replica = replica_list.pop()
                    except IndexError:
                        break

                    list_chunk.append(replica)
                    deletion_size += replica.size()

                deletion_mapping.update(self.transaction_manager.deletion.schedule_deletions(list_chunk, comments = comment, is_test = is_test))

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

    # at the moment partitions are set by groups
    group = inventory_manager.groups[args.partition]

    quotas = {}
    for site in inventory_manager.sites.values():
        quotas[site] = site.group_quota(group)

    policy = Policy(Policy.DEC_PROTECT, [action_list], Policy.ST_GREEDY, quotas, partition = args.partition, replica_requirement = BelongsTo(group))

    detox.set_policy(policy)

    if args.dry_run:
        config.read_only = True

    detox.run(partition = args.partition, is_test = not args.production_run)
