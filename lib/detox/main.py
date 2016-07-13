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

    def set_policy(self, policy, partition = ''): # empty partition name -> default
        self.policies[partition] = policy

    def run(self, partition = '', iterative_deletion = True, is_test = False):
        """
        Main executable.
        """

        if partition:
            logger.info('Detox run for %s starting at %s', partition, time.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            logger.info('Detox run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

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

        replicas = [] # all replicas that the policy considers
        partition_replicas = [] # cloned list of dataset replicas that contain only the block replicas in the partition
        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                applies = policy.applies(replica)
                if applies == 0: # not at all in the partition
                    continue

                replicas.append(replica)

                drep = replica.clone()
                if applies == 2: # replica not fully in the partition; remove parts that are not
                    drep.is_partial = True # partial for the partition
                    for brep in list(drep.block_replicas):
                        if not policy.block_applies(brep):
                            drep.block_replicas.remove(brep)

                partition_replicas.append(drep)

        # take a snapshot of current replicas
        self.history.save_replicas(run_number, partition_replicas)

        logger.info('Start deletion. Evaluating %d rules against %d replicas.', len(policy.rules), len(replicas))

        # Ask each site if deletion should be triggered
        target_sites = set()
        for site in self.inventory_manager.sites.values():
            if policy.need_deletion(site, initial = True):
                target_sites.add(site)

        protected = {} # {replica: reason}
        deleted = {}
        kept = {}

        iteration = 0

        while True:
            iteration += 1

            eval_results = parallel_exec(lambda r: policy.evaluate(r, self.demand_manager), replicas, get_output = True, per_thread = 100)

            deletion_candidates = {} # {replica: reason}

            for replica, decision, reason in eval_results:
                if decision == Policy.DEC_PROTECT:
                    replicas.remove(replica)
                    protected[replica] = reason

                elif replica.site not in target_sites:
                    kept[replica] = 'Site does not need deletion.'

                else:
                    deletion_candidates[replica] = reason

            del eval_results

            logger.info('Iteration %d', iteration)
            logger.info(' %d dataset replicas in deletion candidates', len(deletion_candidates))
            logger.info(' %d dataset replicas in protection list', len(protected))

            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                logger.debug(pprint.pformat(['%s:%s' % (rep.site.name, rep.dataset.name) for rep in deletion_candidates.keys()]))

            if iterative_deletion:
                iter_deletion = self.select_replicas(policy, deletion_candidates.keys(), protected.keys())

                if logger.getEffectiveLevel() == logging.INFO:
                    for replica in iter_deletion:
                        logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)

            else:
                iter_deletion = deletion_candidates.keys()

            if len(iter_deletion) == 0:
                for replica, reason in deletion_candidates.items():
                    kept[replica] = 'CANCELED:', reason

                break

            for replica in iter_deletion:
                deleted[replica] = deletion_candidates[replica]

            # take out replicas from inventory
            # we will not consider deleted replicas
            for replica in iter_deletion:
                self.inventory_manager.unlink_datasetreplica(replica)
                replicas.remove(replica)

            if not iterative_deletion:
                break

            # update the list of target sites
            for site in self.inventory_manager.sites.values():
                if site in target_sites and not policy.need_deletion(site):
                    target_sites.remove(site)

        # save replica snapshots and all deletion decisions
        self.history.save_deletion_decisions(run_number, protected, deleted, kept)

        logger.info('Committing deletion.')
        self.commit_deletions(run_number, policy, deleted.keys(), is_test)

        del protected
        del deleted
        del kept

        self.history.close_deletion_run(run_number)

        logger.info('Detox run finished at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))

    def commit_deletions(self, run_number, policy, deletion_list, is_test):
        # first make sure the list of blocks is up-to-date
        datasets = list(set(r.dataset for r in deletion_list))
        self.inventory_manager.dataset_source.set_dataset_constituent_info(datasets)

        sites = set(r.site for r in deletion_list)

        # now schedule deletion for each site
        for site in sorted(sites):
            replica_list = [r for r in deletion_list if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            sigint.block()

            deletion_mapping = self.transaction_manager.deletion.schedule_deletions(replica_list, groups = policy.groups, comments = self.deletion_message, is_test = is_test)
            # deletion_mapping .. {deletion_id: (approved, [replicas])}

            for deletion_id, (approved, replicas) in deletion_mapping.items():
                if approved and not is_test:
                    self.inventory_manager.store.delete_datasetreplicas(replicas)
                    self.inventory_manager.store.set_last_update()

                size = sum([r.size(policy.groups) for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            del deletion_mapping

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
            protection_by_site = collections.defaultdict(list)
            for replica in protection_list:
                protection_by_site[replica.site].append(replica)

            # find the site with the highest protected fraction
            target_site = max(candidate_sites, key = lambda site: 0 if policy.quotas[site] == 0 else sum(replica.size(policy.groups) for replica in protection_by_site[site]) / policy.quotas[site])
        else:
            target_site = random.choice(candidate_sites)

        sorted_candidates = policy.sort_deletion_candidates([rep for rep in candidate_list if rep.site == target_site], self.demand_manager)

        # return the smallest replica on the target site
        return sorted_candidates[:detox_config.deletion_per_iteration]


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from detox.rules import ActionList

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
        quotas[site] = site.group_quota[group]

    policy = Policy(Policy.DEC_PROTECT, [action_list], quotas, partition = args.partition)
    policy.groups = [group]

    detox.set_policy(policy, partition = args.partition)

    if args.dry_run:
        config.read_only = True

    detox.run(partition = args.partition, iterative_deletion = False, is_test = not args.production_run)
