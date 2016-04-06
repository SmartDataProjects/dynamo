import time
import logging
import datetime

import common.configuration as config
import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class Dealer(object):

    def __init__(self, inventory, transaction, demand):
        self.inventory_manager = inventory
        self.transaction_manager = transaction
        self.demand_manager = demand

        self.copy_message = 'DynaMO -- Automatic Replication Request.'

    def run(self):
        """
        1. Update the inventory if necessary.
        2. Update popularity.
        3. Create pairs of (new replica, source) representing copy operations that should take place.
        4. Execute copy.
        """
        
        logger.info('Dealer run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        if time.time() - self.inventory_manager.store.last_update > config.inventory.refresh_min:
            logger.info('Inventory was last updated at %s. Reloading content from remote sources.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.inventory_manager.store.last_update)))
            # inventory is stale -> update
            self.inventory_manager.update()

        self.demand_manager.update(self.inventory_manager)

        copy_list = self.determine_copies()

        self.commit_copies(copy_list)

    def determine_copies(self):
        """
        Simplest algorithm:
        1. Compute time- and slot- normalized CPU hour (site occupation rate) for each dataset replica.
        2. Sum up occupancy for each site.
        3. At each site, datasets with occupation rate > threshold are placed in the list of copy candidates.
        #5. Sum up occupation rates for each dataset.
        #6. Datasets whose global occupation rates are increasing are placed in the list of copy candidates.
        """

        copy_list = []

        mean_cpu = sum([s.cpu for s in self.inventory_manager.sites.values()]) / len(self.inventory_manager.sites)

        today = datetime.date.today()
        last_three_days = [today - datetime.timedelta(2), today - datetime.timedelta(1), today]

        group = self.inventory_manager.groups['AnalysisOps']

        busy_replicas = []
        site_occupancies = {}

        for site in self.inventory_manager.sites.values():
            if site.cpu == 0.:
                site_cpu = mean_cpu
            else:
                site_cpu = site.cpu

            site_occupancy = 0.
                
            for replica in site.dataset_replicas:
                accesses = replica.accesses[DatasetReplica.ACC_LOCAL]

                dataset_ncpu = 0.
                for date in last_three_days:
                    try:
                        access = accesses[date]
                    except KeyError:
                        continue

                    if date == today:
                        time_norm = self.demand_manager.time_today
                    else:
                        time_norm = 24. * 3600.

                    ncpu = access.cputime / time_norm

                    dataset_ncpu += ncpu
                    site_occupancy += ncpu / site_cpu

                if dataset_ncpu / site_cpu > dealer_config.max_occupation_rate and len(replica.dataset.replicas) <= dealer_config.max_replicas:
                    busy_replicas.append((replica, dataset_ncpu))

            if site.storage_occupancy(group) < config.target_site_occupancy:
                site_occupancies[site] = site_occupancy

        for replica, dataset_ncpu in busy_replicas:
            sorted_sites = sorted(site_occupancies.items(), key = lambda (s, o): o) #sorted from emptiest to busiest

            try:
                # next site in the sorted list that does not have the dataset
                destination_site = next(entry[0] for entry in sorted_sites if entry[0].find_dataset_replica(replica.dataset) is None)
            except StopIteration:
                logger.info('No site to copy %s was found.', replica.dataset.name)

            new_replica = self.inventory_manager.add_dataset_to_site(replica.dataset, destination_site, group)
            copy_list.append((new_replica, replica.site))
            
            if destination_site.storage_occupancy(group) > config.target_site_occupancy:
                # site is full; not considered as destination candidate any more
                site_occupancies.pop(destination_site)
                continue

            # add the cpu usage of this replica to destination occupancy
            if destination_site.cpu == 0.:
                site_cpu = mean_cpu
            else:
                site_cpu = destination_site.cpu

            site_occupancies[destination_site] += dataset_ncpu / site_cpu

        return copy_list

    def commit_copies(self, copy_list):
        for replica, origin in copy_list:
            self.transaction_manager.copy.schedule_copy(replica.dataset, replica.site, origin, comments = self.copy_message)

        self.inventory_manager.store.add_dataset_replicas([replica for replica, origin in copy_list])
