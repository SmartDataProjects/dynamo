import time
import logging
import math
import pprint
import collections
import random
import sys

import common.configuration as config
from common.dataformat import DatasetReplica, BlockReplica
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

        self.deletion_message = 'Dynamo -- Automatic Cache Release Request.'

    def set_policy(self, policy):
        """
        Can be called multiple times to set policies for different partitions.
        """

        self.policies[policy.partition] = policy

    def run(self, partition = '', is_test = False):
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

        self.demand_manager.update(self.inventory_manager, accesses = True, requests = False)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        policy = self.policies[partition]

        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(partition, is_test = is_test)

        logger.info('Preparing deletion run %d', run_number)

        # update site and dataset lists
        # take a snapshot of site status
        self.history.save_sites(run_number, self.inventory_manager)
        self.history.save_datasets(run_number, self.inventory_manager)
        # take snapshots of quotas if updated
        self.history.save_quotas(run_number, partition, policy.quotas, self.inventory_manager)

        logger.info('Identfying target sites.')

        # Ask each site if deletion should be triggered
        target_sites = set()
        for site in self.inventory_manager.sites.values():
            if policy.need_deletion(site, initial = True):
                target_sites.add(site)

        logger.info('Identfying dataset replicas in the partition.')

        # "partition" as a verb - selecting only the blockreps in the partition
        all_replicas = policy.partition_replicas(self.inventory_manager.datasets.values())

        # take a snapshot of current replicas
        self.history.save_replicas(run_number, all_replicas)

        logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(all_replicas))

        protected = {} # {replica: reason}
        deleted = {}
        kept = {}

        iteration = 0

        while True:
            iteration += 1

            eval_results = parallel_exec(lambda r: policy.evaluate(r, self.demand_manager.dataset_demands[r.dataset]), all_replicas, per_thread = 100)

            deletion_candidates = {} # {replica: reason}

            for replica, decision, reason in eval_results:
                if decision == Policy.DEC_PROTECT:
                    all_replicas.remove(replica)
                    protected[replica] = reason

                elif replica.site not in target_sites:
                    kept[replica] = 'Site does not need deletion.'

                else:
                    deletion_candidates[replica] = reason

            del eval_results

            if policy.strategy == Policy.ST_ITERATIVE:
                logger.info('Iteration %d', iteration)

            logger.info(' %d dataset replicas in deletion candidates', len(deletion_candidates))
            logger.info(' %d dataset replicas in protection list', len(protected))

            if len(deletion_candidates) == 0:
                break

            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                logger.debug(pprint.pformat(['%s:%s' % (rep.site.name, rep.dataset.name) for rep in deletion_candidates.keys()]))

            if policy.strategy == Policy.ST_ITERATIVE:
                # Pick out the replicas to delete in this iteration, unlink the replicas, and update the list of target sites.

                iter_deletion = self.select_replicas(policy, deletion_candidates.keys(), protected.keys())

                if logger.getEffectiveLevel() == logging.DEBUG:
                    for replica in iter_deletion:
                        logger.debug('Selected replica: %s %s', replica.site.name, replica.dataset.name)

                for replica in iter_deletion:
                    deleted[replica] = deletion_candidates[replica]

                    # take out replicas from inventory
                    # we will not consider deleted replicas    
                    self.inventory_manager.unlink_datasetreplica(replica)
                    all_replicas.remove(replica)

                # update the list of target sites
                for site in self.inventory_manager.sites.values():
                    if site in target_sites and not policy.need_deletion(site):
                        target_sites.remove(site)

            elif policy.strategy == Policy.ST_STATIC:
                # Delete the replicas site-by-site in the order given by the policy until the site does not need any more deletion.

                for site in target_sites:
                    sorted_candidates = policy.sort_deletion_candidates([(rep, self.demand_manager.dataset_demands[rep.dataset]) for rep in deletion_candidates if rep.site == site])
                    # from the least desirable (to delete) to the most

                    while len(sorted_candidates) != 0:
                        replica = sorted_candidates.pop()
                        self.inventory_manager.unlink_datasetreplica(replica)
                        deleted[replica] = deletion_candidates[replica]
                        if not policy.need_deletion(site):
                            break

                    for replica in sorted_candidates:
                        kept[replica] = 'Site does not need deletion.'

                break

            elif policy.strategy == Policy.ST_GREEDY:
                # Delete all candidates.

                deleted = deletion_candidates

                for replica in deleted.keys():
                    self.inventory_manager.unlink_datasetreplica(replica)

                break

        # save replica snapshots and all deletion decisions
        logger.info('Saving deletion decisions.')
        self.history.save_deletion_decisions(run_number, protected, deleted, kept)

        logger.info('Committing deletion.')
        self.commit_deletions(run_number, policy, deleted.keys(), is_test)

        policy.restore_replicas()

        # remove datasets that lost replicas
        for dataset in set([replica.dataset for replica in deleted.keys()]):
            if len(dataset.replicas) == 0:
                if not is_test:
                    self.inventory_manager.store.delete_dataset(dataset)
                    self.inventory_manager.datasets.pop(dataset.name)

                dataset.unlink()

        self.history.close_deletion_run(run_number)

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def commit_deletions(self, run_number, policy, deletion_list, is_test):
        sites = set(r.site for r in deletion_list)

        # now schedule deletion for each site
        for site in sorted(sites):
            replica_list = [r for r in deletion_list if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            sigint.block()

            deletion_mapping = self.transaction_manager.deletion.schedule_deletions(replica_list, comments = self.deletion_message, is_test = is_test)
            # deletion_mapping .. {deletion_id: (approved, [replicas])}

            for deletion_id, (approved, replicas) in deletion_mapping.items():
                if approved and not is_test:
                    for replica in replicas:
                        self.inventory_manager.store.delete_blockreplicas(replica.block_replicas)
                        if replica not in policy.untracked_replicas:
                            # this replica was fully in the partition
                            # second arg is False because block replicas must be all gone by now
                            self.inventory_manager.store.delete_datasetreplica(replica, delete_blockreplicas = False)

                    self.inventory_manager.store.set_last_update()

                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            sigint.unblock()

            logger.info('Done deleting %d replicas from %s.', len(replica_list), site.name)

    def select_replicas(self, policy, candidate_list, protection_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        """

        if len(candidate_list) == 0:
            return {}

        candidate_sites = list(set(rep.site for rep in candidate_list))

        if len(protection_list) != 0:
            protection_by_site = dict([(site, 0.) for site in candidate_sites])
            for replica in protection_list:
                if replica.site in candidate_sites:
                    protection_by_site[replica.site] += replica.size()

            # find the site with the highest protected fraction
            target_site = max(candidate_sites, key = lambda site: 0 if policy.quotas[site] == 0 else protection_by_site[site] / policy.quotas[site])
        else:
            target_site = random.choice(candidate_sites)

        sorted_candidates = policy.sort_deletion_candidates([(rep, self.demand_manager.dataset_demands[rep.dataset]) for rep in candidate_list if rep.site == target_site])
        # from the least desirable (to delete) to the most

        if policy.quotas[target_site] == 0:
            return [sorted_candidates[-1]]

        else:
            selected_replicas = []
            while sum(replica.size() for replica in selected_replicas) / policy.quotas[target_site] < detox_config.deletion_per_iteration:
                selected_replicas.append(sorted_candidates.pop())

            return selected_replicas


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from detox.rules import ActionList, BelongsTo

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
