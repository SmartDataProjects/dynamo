import time
import logging

import common.configuration as config
from detox.policy import Policy, PolicyManager
import detox.configuration as detox_config

logger = logging.getLogger(__name__)

class Detox(object):

    def __init__(self, inventory, transaction, demand, policies, log_path = detox_config.log_path):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand
        self.policy_manager = PolicyManager(policies)
        self.policy_log_path = log_path

    def run(self):
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
            deletion_list = self.make_deletion_list(policy_log)

            logger.info('%d dataset replicas in deletion list', len(deletion_list))
            logger.debug('[%s]', ', '.join(['%s:%s' % (r.site.name, r.dataset.name) for r in deletion_list]))

            if len(deletion_list) == 0:
                break

            replica = self.select_replica(deletion_list)
            logger.info('Selected replica: %s %s', replica.site.name, replica.dataset.name)

            self.transaction_manager.delete(replica)
            self.inventory_manager.delete_replica(replica)

        policy_log.close()

        logger.info('Detox run finished at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

    def make_deletion_list(self, policy_log = None):
        """
        Run each dataset / block replicas through deletion policies and make a list of replicas to delete.
        """

        deletion_list = []

        for dataset in self.inventory_manager.datasets.values():
            for replica in dataset.replicas:
                decision, hit_records = self.policy_manager.decision(replica, self.demand_manager)
                if policy_log:
                    hit_records.write_records(policy_log)

                if decision == Policy.DEC_DELETE:
                    deletion_list.append(replica)
                
        return deletion_list

    def select_replica(self, deletion_list):
        """
        Select one dataset replica to delete out of all deletion candidates.
        Currently returning the largest replica on the most occupied site.
        Ranking policy here may be made dynamic at some point.
        """

        if len(deletion_list) == 0:
            return None

        most_occupied_site = None
        largest_replica_on_site = {}

        for replica in deletion_list:
            if most_occupied_site is None or replica.site.occupancy() > most_occupied_site.occupancy():
                most_occupied_site = replica.site

            try:
                max_size = largest_replica_on_site[replica.site][0]
            except KeyError:
                max_size = 0

            size = replica.size()
            if size >= max_size:
                largest_replica_on_site[replica.site] = (size, replica)

        return largest_replica_on_site[most_occupied_site][1]
