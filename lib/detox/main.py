import time
import logging
import math
import pprint
import collections

import common.configuration as config
import detox.policy as policy
import detox.configuration as detox_config

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, history):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.history = history

        self.policy_managers = {} # {partition: PolicyManager}
        self.quotas = {} # {partition: {site: quota}}

        self.deletion_message = 'Dynamo -- Automatic Cache Release Request.'

    def set_policies(self, policy_stack, quotas, partition = ''): # empty partition name -> default
        self.policy_managers[partition] = policy.PolicyManager(policy_stack)
        self.quotas[partition] = quotas

    def run(self, partition = '', dynamic_deletion = True, is_test = False):
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

        self.demand_manager.update(self.inventory_manager)
        self.inventory_manager.site_source.set_site_status(self.inventory_manager.sites) # update site status regardless of inventory updates

        all_deletions = []
        iteration = 0

        logger.info('Start deletion. Evaluating %d policies against %d replicas.', self.policy_managers[partiton].num_policies(), sum([len(d.replicas) for d in self.inventory_manager.datasets.values()]))

        protection_list = []

        while True:
            iteration += 1

            records = []
            candidates = self.find_candidates(partition, protection_list, records)

            if policy_log:
                logger.info('Writing policy hit log text.')
                self.write_policy_log(policy_log, iteration, records)

            logger.info('%d dataset replicas in deletion list', len(candidates))
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                logger.debug(pprint.pformat(['%s:%s' % (r.site.name, r.dataset.name) for r in candidates]))

            if dynamic_deletion:
                replica = self.select_replica(partition, candidates, protection_list)
                if replica is None:
                    deletion_list = []
                else:
                    logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)
                    deletion_list = [replica]

            else:
                deletion_list = candidates

            if len(deletion_list) == 0:
                break

            all_deletions.extend(deletion_list)

            for replica in deletion_list:
                self.inventory_manager.unlink_datasetreplica(replica)

        # fetch the copy/deletion run number
        run_number = self.history.new_deletion_run(partition, is_test = is_test)

        # update site and dataset lists
        self.history.save_sites(self.inventory_manager)
        self.history.save_datasets(self.inventory_manager)
        # take snapshots of quotas if updated
        self.history.save_quotas(run_number, self.quotas, self.inventory_manager)
        # save replica snapshots and all deletion decisions
        self.history.save_deletion_decisions(run_number, all_deletions, protection_list, self.inventory_manager)

        logger.info('Committing deletion.')
        self.commit_deletions(run_number, all_deletions, is_test)

        if policy_log:
            policy_log.close()

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def find_candidates(self, partition, protection_list, records = None):
        """
        Run each dataset / block replicas through deletion policies and make a list of replicas to delete.
        Return the list of replicas that may be deleted (deletion_candidates) or must be protected (protection_list).
        """

        candidates = []

        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                if replica in protection_list:
                    continue

                hit_records = self.policy_managers[partition].decision(replica, self.demand_manager)

                if records is not None:
                    records.append(hit_records)

                decision = hit_records.decision()

                if decision == policy.DEC_DELETE:
                    candidates.append(replica)

                elif decision == policy.DEC_PROTECT:
                    protection_list.append(replica)
                
        return candidates

    def commit_deletions(self, run_number, all_deletions, is_test):
        # first make sure the list of blocks is up-to-date
        datasets = list(set(r.dataset for r in all_deletions))
        self.inventory_manager.dataset_source.set_dataset_constituent_info(datasets)

        sites = set(r.site for r in all_deletions)

        # now schedule deletion for each site
        for site in sorted(sites):
            replica_list = [r for r in all_deletions if r.site == site]

            logger.info('Deleting %d replicas from %s.', len(replica_list), site.name)

            deletion_mapping = self.transaction_manager.deletion.schedule_deletions(replica_list, comments = self.deletion_message, is_test = is_test)
            # deletion_mapping .. {deletion_id: (approved, [replicas])}

            for deletion_id, (approved, replicas) in deletion_mapping.items():
                if approved and not is_test:
                    self.inventory_manager.store.delete_datasetreplicas(replicas)
                    self.inventory_manager.store.set_last_update()

                size = sum([r.size() for r in replicas])

                self.history.make_deletion_entry(run_number, site, deletion_id, approved, [r.dataset for r in replicas], size)

            logger.info('Done deleting %d replicas from %s.', len(replica_list), site.name)

    def select_replica(self, partition, deletion_candidates, protection_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        Currently returning the smallest replica on the site with the highest protected fraction.
        Ranking policy here may be made dynamic at some point.
        """

        if len(deletion_candidates) == 0:
            return None

        protection_by_site = collections.defaultdict(list)
        for replica in protection_list:
            protection_by_site[replica.site].append(replica)

        # find the site with the highest protected fraction
        target_site = max(protection_by_site.keys(), key = lambda site: sum(replica.size() for replica in protection_by_site[site]) / self.quotas[partition][site])

        # return the smallest replica on the target site
        return min(protection_by_site[target_site], key = lambda replica: replica.size())

if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    from common.inventory import InventoryManager
    from common.transaction import TransactionManager
    from common.demand import DemandManager
    import common.interface.classes as classes
    from detox.policies import ActionList

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
    quotas = {}
    for group in inventory_manager.groups.values():
        group_quotas = {}
        for site in inventory_manager.sites.values():
            group_quotas[site] = site.quota(group)

        quotas[group.name] = group_quotas

    detox.set_policies(action_list, quotas)

    if args.dry_run:
        config.read_only = True

    detox.run(partition = args.partition, dynamic_deletion = False, is_test = not args.production_run)
