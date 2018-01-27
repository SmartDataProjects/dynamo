import logging
import re
import fnmatch
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

    def get_requests(self, inventory, history, policy): # override
        requests = []

        partition = inventory.partitions[policy.partition_name]

        for rule_name, rule in self.policy.rules.iteritems():
            # split up sites into considered ones and others
            sites_considered = set()
            sites_others = set()

            target_num = rule['num_copies']

            site_patterns = []
            for pattern in rule['sites']:
                site_patterns.append(re.compile(fnmatch.translate(pattern)))

            dataset_patterns = []
            for pattern in rule['datasets']:
                dataset_patterns.append(re.compile(fnmatch.translate(pattern)))

            for site in inventory.sites.values():
                quota = site.partitions[partition].quota

                LOG.debug('Site %s quota %f TB', site.name, quota * 1.e-12)

                if quota <= 0:
                    # if the site has 0 or infinite quota, don't consider in enforcer
                    continue

                for pattern in site_patterns:
                    if pattern.match(site.name):
                        sites_considered.add(site)
                        break
                else:
                    sites_others.add(site)

            if target_num > len(sites_considered):
                # This is never fulfilled - cap
                target_num = len(sites_considered)

            checked_datasets = set()

            # Create a request for datasets that has at least one copy in sites_others and less than
            # [target_num] copy in sites_considered

            for site in sites_others:
                for replica in site.partitions[partition].replicas.iterkeys():
                    dataset = replica.dataset

                    if dataset in checked_datasets:
                        continue

                    checked_datasets.add(dataset)

                    if dataset.size > self.max_dataset_size:
                        continue

                    for pattern in dataset_patterns:
                        if pattern.match(dataset.name):
                            break
                    else:
                        continue

                    num_considered = 0

                    for other_replica in dataset.replicas:
                        if other_replica is replica:
                            continue

                        if other_replica.site in sites_considered:
                            num_considered += 1
                            if num_considered == target_num:
                                break

                    else:
                        # num_considered did not hit target_num
                        # create a request

                        site_candidates = sites_considered - set(r.site for r in dataset.replicas if r.is_full())
                        if len(site_candidates) != 0:
                            # can be 0 if the dataset has copies in other partitions
                            target_site = random.choice(list(site_candidates))

                            LOG.debug('Enforcer rule %s requesting %s at %s', rule_name, dataset.name, target_site.name)
                            requests.append((dataset, target_site))

        # randomize requests
        random.shuffle(requests)

        return requests
