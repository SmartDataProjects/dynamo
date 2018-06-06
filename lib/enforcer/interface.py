import logging
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

        self.protect = config.get('protect', False)


class EnforcerInterface(object):
    """
    Interface for obtaining infos from enforcer--the requests themselves
    or info for writing rrd files
    """

    def __init__(self, config):
        policy_conf = Configuration(config.policy)

        # Partition to work in
        self.partition_name = policy_conf.partition

        # Enforcer policies
        self.rules = {}
        for rule_name, rule in policy_conf.rules.iteritems():
            self.rules[rule_name] = EnforcerRule(rule)

        # If True, report_back returns a list to be fed to RRD writing
        self.write_rrds = config.get('write_rrds', False)

        # Not considering datasets larger than this value.
        self.max_dataset_size = config.get('max_dataset_size', 0.) * 1.e+12

    def report_back(self, inventory):
        """
        The main enforcer logic for the replication part.
        @param inventory        Current status of replica placement across system
        """
        
        partition = inventory.partitions[self.partition_name]
        
        product = []

        for rule_name, rule in self.rules.iteritems():
            # split up sites into considered ones and others

            destination_sites = self.get_destination_sites(rule_name, inventory, partition)
            source_sites = self.get_source_sites(rule_name, inventory, partition)

            target_num = rule.num_copies

            already_there = []
            en_route = {}
            still_missing = {}

            if target_num > len(destination_sites):
                # This is never fulfilled - cap
                target_num = len(destination_sites)

            checked_datasets = set()

            # Create a request for datasets that has at least one copy in source_sites and less than
            # [target_num] copy in destination_sites

            for site in source_sites:
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

                    num_complete = 0
                    num_incomplete = 0

                    for other_replica in dataset.replicas:
                        if other_replica is replica:
                            continue

                        if other_replica.site in destination_sites:
                            if other_replica.is_complete():
                                num_complete += 1
                            else:
                                num_incomplete += 1

                    if num_complete >= target_num:
                        already_there.append(replica.dataset.name)
                    elif num_complete + num_incomplete >= target_num:
                        en_route[replica.dataset.name + "__" + site.name] = str(replica.size(physical = True)) + "__" + str(replica.size(physical = False))
                    else:
                        # create a request

                        site_candidates = destination_sites - set(r.site for r in dataset.replicas if r.is_full())
                        if len(site_candidates) != 0:
                            # can be 0 if the dataset has copies in other partitions

                            still_missing[replica.dataset.name] = replica.dataset.size

                            if not self.write_rrds:
                                target_site = random.choice(list(site_candidates))
    
                                LOG.debug('Enforcer rule %s requesting %s at %s', rule_name, dataset.name, target_site.name)
                                product.append((dataset, target_site))

            if self.write_rrds:
                product.append((rule_name, already_there, en_route, still_missing))

        if not self.write_rrds:
            # randomize requests
            random.shuffle(product)

        return product

    def get_destination_sites(self, rule_name, inventory, partition):
        rule = self.rules[rule_name]
        return self._get_sites(rule.destination_sites, inventory, partition)

    def get_source_sites(self, rule_name, inventory, partition):
        rule = self.rules[rule_name]
        return self._get_sites(rule.source_sites, inventory, partition)

    def _get_sites(self, conditions, inventory, partition):
        sites = set()

        for site in inventory.sites.values():
            site_partition = site.partitions[partition]
                
            for condition in conditions:
                if condition.match(site_partition):
                    sites.add(site)
                    break

        return sites
