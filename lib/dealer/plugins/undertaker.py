import logging

from dynamo.dataformat import Site
from base import BaseHandler

LOG = logging.getLogger(__name__)

class Undertaker(BaseHandler):
    def __init__(self, config):
        BaseHandler.__init__(self, 'Undertaker')

    def get_requests(self, inventory, history, policy): # override
        latest_runs = history.get_deletion_runs(policy.partition_name)
        if len(latest_runs) == 0:
            return []

        latest_run = latest_runs[0]

        LOG.info('Offloading sites that were not in READY state at latest cycle %d', latest_run)

        deletion_decisions = history.get_deletion_decisions(latest_run, size_only = False)

        protected_fractions = {} # {site: fraction}
        last_copies = {} # {site: [datasets]}

        bad_sites = [site for site in inventory.sites.values() if site.status != Site.STAT_READY]

        requests = []

        total_size = 0.

        for site in bad_sites:
            try:
                decisions = deletion_decisions[site.name]
            except KeyError:
                continue

            for ds_name, size, decision, reason in decisions:
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
                        requests.append(dataset)
                        total_size += dataset.size
                    else:
                        requests.append(list(blocks_only_at_site))
                        total_size += sum(b.size for b in blocks_only_at_site)
    
        LOG.info('Offloading protected datasets from non-ready sites %s (total size %.1f TB)', str([s.name for s in bad_sites]), total_size * 1.e-12)

        return requests

    def save_record(self, run_number, history, copy_list): # override
        pass
