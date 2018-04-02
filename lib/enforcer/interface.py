import logging
import re
import fnmatch
import random

from dynamo.dataformat import Configuration
from dynamo.policy.condition import Condition
from dynamo.policy.variables import replica_variables, site_variables

LOG = logging.getLogger(__name__)

class EnforcerRule(object):
    def __init__(self, config):
        self.num_copies = config.num_copies

        self.destination_sites = [] # list of ORed conditions
        for cond_text in config.destinations:
            self.destination_sites.append(Condition(cond_text, site_variables))

        self.source_sites = [] # list of ORed conditions
        for cond_text in config.sources:
            self.source_sites.append(Condition(cond_text, site_variables))
        
        self.target_replicas = [] # list of ORed conditions
        for cond_text in config.replicas:
            self.target_replicas.append(Condition(cond_text, replica_variables))


class EnforcerInterface(object):
    """
    Interface for obtaining infos from enforcer--the requests themselves
    or info for writing rrd files
    """

    def __init__(self, config):
        # If True, report_back returns a list to be fed to RRD writing
        self.write_rrds = config.get('write_rrds', False)
        # Not considering datasets larger than this value.
        self.max_dataset_size = config.max_dataset_size * 1.e+12
        # Enforcer policies
        self.rules = {}
        for rule_name, rule in Configuration(config.rules).iteritems():
            self.rules[rule_name] = EnforcerRule(rule)

    def report_back(self, inventory, partition):
        """
        The main enforcer logic for the replication part.
        @param inventory        Current status of replica placement across system
        @param partition        Which partition do we want to consider?
        """
        
        product = []

        for rule_name, rule in self.rules.iteritems():
            # split up sites into considered ones and others

            sites_considered = set()
            sites_others = set()

            target_num = rule.num_copies

            already_there = 0
            still_missing = 0

            for site in inventory.sites.values():
                site_partition = site.partitions[partition]
                quota = site_partition.quota

                LOG.debug('Site %s quota %f TB', site.name, quota * 1.e-12)

                for condition in rule.destination_sites:
                    if condition.match(site_partition):
                        LOG.debug('Site %s matches the destination spec', site.name)
                        sites_considered.add(site)
                        break
                    
                for condition in rule.source_sites:
                    if condition.match(site_partition):
                        LOG.debug('Site %s matches the source spec', site.name)
                        sites_others.add(site)
                        break

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

                    for condition in rule.target_replicas:
                        if condition.match(replica):
                            break
                    else:
                        # no condition matched
                        continue

                    num_considered = 0

                    for other_replica in dataset.replicas:
                        if other_replica is replica:
                            continue

                        if other_replica.site in sites_considered:
                            num_considered += 1
                            if num_considered == target_num:
                                already_there += 1
                                break

                    else:
                        # num_considered did not hit target_num
                        # create a request

                        site_candidates = sites_considered - set(r.site for r in dataset.replicas if r.is_full())
                        if len(site_candidates) != 0:
                            # can be 0 if the dataset has copies in other partitions

                            still_missing += 1

                            if not self.write_rrds:
                                target_site = random.choice(list(site_candidates))
    
                                LOG.debug('Enforcer rule %s requesting %s at %s', rule_name, dataset.name, target_site.name)
                                product.append((dataset, target_site))

            if self.write_rrds:
                product.append((rule_name, already_there, still_missing))

        if not self.write_rrds:
            # randomize requests
            random.shuffle(product)

        return product
