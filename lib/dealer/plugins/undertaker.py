import logging

from dynamo.dataformat import Site
from base import BaseHandler, DealerRequest
from dynamo.detox.history import DetoxHistoryBase

LOG = logging.getLogger(__name__)

class Undertaker(BaseHandler):
    def __init__(self, config):
        BaseHandler.__init__(self, 'Undertaker')

        self.manual_evacuation_sites = list(config.get('additional_sites', []))
        self.detoxhistory = DetoxHistoryBase(config.get('detox_history', None))

    def get_requests(self, inventory, policy): # override
        latest_cycles = self.detoxhistory.get_cycles(policy.partition_name)
        if len(latest_cycles) == 0:
            return []

        latest_cycle = latest_cycles[0]

        LOG.info('Offloading sites that were not in READY state at latest cycle %d', latest_cycle)
        if len(self.manual_evacuation_sites) != 0:
            LOG.info('Additionally evacuating %s as requested by configuration', ' '.join(self.manual_evacuation_sites))

        deletion_decisions = self.detoxhistory.get_deletion_decisions(latest_cycle, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        bad_sites = set(site for site in inventory.sites.values() if site.status != Site.STAT_READY)
        bad_sites.update(inventory.sites[s] for s in self.manual_evacuation_sites)

        requests = []

        total_size = 0.

        for site in bad_sites:
            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            for ds_name, size, decision, _, _ in decisions:
                if decision != 'protect':
                    continue

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    continue

                if dataset.replicas is None:
                    continue

                site_replica = dataset.find_replica(site)

                if site_replica is None:
                    # this dataset is no more at site
                    continue

                # are there blocks at site that are nowhere else?

                covered_blocks = set()
                for replica in dataset.replicas:
                    if replica == site_replica or replica.site in bad_sites:
                        continue

                    covered_blocks.update(br.block for br in replica.block_replicas)

                blocks_on_site = set(br.block for br in site_replica.block_replicas)

                blocks_only_at_site = blocks_on_site - covered_blocks

                if len(blocks_only_at_site) != 0:
                    LOG.debug('%s has a last copy block at %s', ds_name, site.name)

                    if blocks_only_at_site == set(dataset.blocks):
                        # the entire dataset needs to be transferred off
                        requests.append(DealerRequest(dataset))
                        total_size += dataset.size
                    else:
                        requests.append(DealerRequest(list(blocks_only_at_site)))
                        total_size += sum(b.size for b in blocks_only_at_site)
    
        LOG.info('Offloading protected datasets from sites [%s] (total size %.1f TB)', ' '.join(s.name for s in bad_sites), total_size * 1.e-12)

        return requests
