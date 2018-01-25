import logging
import re
import random

from base import BaseHandler
from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)

class EnforcerHandler(BaseHandler):
    """
    Request replication of datasets using custom rules and destinations.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Enforcer')

        self.policy = Configuration(config.policy)
        self.max_dataset_size = config.max_dataset_size * 1.e+12

    def get_requests(self, inventory, policy): # override
        requests = []

        for rule in self.policy.rules:
            # split up sites into considered ones and others
            sites_considered = []
            sites_others = []

            for site in inventory.sites.values():
                quota = site.partitions[partition].quota

                LOG.debug('Site %s quota %f TB', site.name, quota * 1.e-12)

                if quota <= 0:
                # if the site has 0 or infinite quota, don't consider in enforcer
                    continue

                site_considered = False
                for sitename in rule['sites']:
                    pattern = re.compile(sitename.replace("*","[^\s]*"))
                    if pattern.match(site.name):
                        site_considered = True
                if site_considered:
                    sites_considered.append(site)
                else:
                    sites_others.append(site)

            # How many copies already present at considered/other sites?
            for dataset in inventory.datasets.values():
                if dataset.size > self.max_dataset_size:
                    continue

                pattern = re.compile(rule['datasets'].replace("*","[^\s]*"))
                if not pattern.match(dataset.name):
                    continue

                num_considered = 0
                for site_considered in sites_considered:
                    if dataset.find_replica(site_considered) is not None:
                        num_considered += 1

                num_others = 0
                for site_other in sites_others:
                    if dataset.find_replica(site_other) is not None:
                        num_others += 1

                if (num_considered < rule['num_copies'] and num_others > 0 
                    and rule['num_copies'] <= len(sites_considered)):
                    random_site = random.choice(sites_considered)

                    while dataset.find_replica(random_site) is not None:
                        random_site = random.choice(sites_considered)

                    requests.append((dataset,random_site))

    return requests

