import time
import logging
import math

import common.configuration as config
import detox.policy as policy
import detox.configuration as detox_config

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, policies, log_path = detox_config.log_path):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.policy_manager = policy.PolicyManager(policies)
        self.policy_log_path = log_path

    def run(self, dynamic_deletion = True):
        """
        Main executable.
        """

        logger.info('Detox run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        policy_log = open(self.policy_log_path, 'w')

        if time.time() - self.inventory_manager.inventory.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.inventory.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        else:
            self.inventory_manager.load()

        self.demand_manager.update(self.inventory_manager.inventory)

        logger.info('Start deletion.')

        while True:
            deletion_list, protection_list = self.make_deletion_protection_list(policy_log)

            logger.info('%d dataset replicas in deletion list', len(deletion_list))
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('Deletion list:')
                for replica in deletion_list:
                    logger.debug('%s %s', replica.site.name, replica.dataset.name)

            if len(deletion_list) == 0:
                break

            if dynamic_deletion:
                replica = self.select_replica(deletion_list, protection_list)
                logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)

                self.transaction_manager.delete(replica)
                self.inventory_manager.delete_replica(replica)

            else:
                print ['%s:%s' % (r.site.name, r.dataset.name) for r in deletion_list]

#                self.transaction_manager.delete_many(deletion_list)
#                for replica in deletion_list:
#                    self.inventory_manager.delete_replica(replica)

                logger.info('Deleted %d replicas.', len(deletion_list))

        policy_log.close()

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def make_deletion_protection_list(self, policy_log = None):
        """
        Run each dataset / block replicas through deletion policies and make a list of replicas to delete.
        Return the list of replicas that may be deleted and must be protected.
        """

        deletion_list = []
        protection_list = []

        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                decision, hit_records = self.policy_manager.decision(replica, self.demand_manager)
                if policy_log:
                    hit_records.write_records(policy_log)

                if decision == policy.DEC_DELETE:
                    deletion_list.append(replica)

                elif decision == policy.DEC_PROTECT:
                    protection_list.append(replica)
                
        return deletion_list, protection_list

    def select_replica(self, deletion_list, protection_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        Currently returning the replica whose deletion balances the protected fraction between the sites the most.
        Ranking policy here may be made dynamic at some point.
        """

        if len(deletion_list) == 0:
            return None
        
        protected_fractions = {}
        for replica in protection_list:
            try:
                protected_fractions[replica.site] += replica.size() / replica.site.capacity
            except KeyError:
                protected_fractions[replica.site] = replica.size() / replica.site.capacity

        for site in self.inventory_manager.sites.values():
            if site not in protected_fractions: # highly unlikely
                protected_fractions[site] = 0.

        # Select the replica that minimizes the RMS of the protected fractions
        minRMS2 = -1.
        deletion_candidate = None

        for replica in deletion_list:
            sumf2 = sum([frac * frac for site, frac in protected_fractions.items() if site != replica.site])
            sumf = sum([frac for site, frac in protected_fractions.items() if site != replica.site])

            sumf2 += math.pow(protected_fractions[replica.site] - replica.size() / replica.site.capacity, 2.)
            sumf += protected_fractions[replica.site] - replica.size() / replica.site.capacity

            rms2 = sumf2 / len(self.inventory_manager.sites) - math.pow(sumf / len(self.inventory_manager.sites), 2.)

            if minRMS2 < 0. or rms2 < minRMS2:
                minRMS2 = rms2
                deletion_candidate = replica

        return deletion_candidate
