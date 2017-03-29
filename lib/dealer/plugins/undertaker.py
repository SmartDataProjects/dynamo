import logging

from dealer.plugins import plugins
from dealer.plugins.base import BaseHandler
from common.interface.mysqlhistory import MySQLHistory
from common.dataformat import Site

logger = logging.getLogger(__name__)

class Undertaker(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

        self.name = 'Undertaker'
        self.history = None

    def get_requests(self, inventory, partition): # override
        if self.history is None:
            return [], [], []

        latest_run = self.history.get_latest_deletion_run(partition.name)

        deletion_decisions = self.history.get_deletion_decisions(latest_run, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        bad_sites = set()
        datasets = []

        total_size = 0.

        for site in inventory.sites.values():
            if site.status == Site.STAT_READY:
                continue

            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            for ds_name, size, decision in decisions:
                if decision != 'protect':
                    continue

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue
    
                if len(dataset.replicas) == 1: # this replica has no other copy
                    logger.debug('%s is a last copy at %s', ds_name, site.name)
                    datasets.append(dataset)

                    total_size += size
    
                    if site not in bad_sites:
                        bad_sites.add(site)

        logger.info('Offloading protected datasets from non-ready sites %s (total size %.1f TB)', str([s.name for s in bad_sites]), total_size * 1.e-12)

        return datasets, [], []

    def save_record(self, run_number, history, copy_list): # override
        pass


plugins['Undertaker'] = Undertaker()
