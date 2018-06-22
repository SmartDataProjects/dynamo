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

        self.destination_group_name = config.get('destination_group', '')

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
        for rule_name, rule_conf in policy_conf.rules.iteritems():
            rule = EnforcerRule(rule_conf)
            if not rule.destination_group_name:
                rule.destination_group_name = policy_conf.default_destination_group

            self.rules[rule_name] = rule

        # If True, report_back returns a list to be fed to RRD writing
        self.write_rrds = config.get('write_rrds', False)

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

            destination_group = inventory.groups[rule.destination_group_name]

            target_num = rule.num_copies

            if self.write_rrds:
                already_there = []
                en_route = {}
                still_missing = {}

            if target_num > len(destination_sites):
                # This is never fulfilled - cap
                target_num = len(destination_sites)

            # list of datasets to evaluate
            datasets_to_evaluate = set()

            # Identify datasets that are in the source sites and should be evaluated
            for site in source_sites:
                site_partition = site.partitions[partition]
                for replica in site_partition.replicas.iterkeys():
                    if replica.dataset in datasets_to_evaluate:
                        # don't need to run the rule conditions any more
                        continue

                    for condition in rule.target_replicas:
                        if condition.match(replica):
                            datasets_to_evaluate.add(replica.dataset)
                            break

            # Check how many full replicas of the dataset are in the destination sites
            for dataset in datasets_to_evaluate:
                num_complete = 0
                num_incomplete = 0
                used_sites = set()
                data_incomplete = {}
                can_be_flipped = []

                # count the replicas already at the destinations
                for replica in dataset.replicas:
                    if replica.site not in destination_sites:
                        continue

                    if len(replica.block_replicas) != len(replica.dataset.blocks):
                        # this is a block-level replica instead of a dataset subscription
                        continue

                    try:
                        blockreps_in_partition = replica.site.partitions[partition].replicas[replica]
                    except KeyError:
                        continue

                    if blockreps_in_partition is not None:
                        # skip replicas not fully in partition
                        continue

                    owners = set(b.group for b in replica.block_replicas)

                    if not (len(owners) == 1 and list(owners)[0] is destination_group):
                        # Replicas not owned by the target group is neither complete nor incomplete
                        # But can be flipped the ownership later as an easy completion
                        can_be_flipped.append(replica)
                        continue

                    used_sites.add(replica.site)

                    if replica.is_full():
                        num_complete += 1
                    else:
                        num_incomplete += 1
                        data_incomplete[replica.site.name] = str(replica.size(physical = True)) + "__" + str(replica.size(physical = False))

                if num_complete >= target_num:
                    if self.write_rrds:
                        already_there.append(dataset.name)
                elif num_complete + num_incomplete >= target_num:
                    if self.write_rrds:
                        for key, value in data_incomplete.items():
                            en_route[dataset.name + "__" + key] = value
                else:
                    # Number of complete and en-route replicas is smaller than the number of replicas needed. We need to create more replicas.

                    site_candidates = destination_sites - used_sites
                    if len(site_candidates) == 0:
                        # we have nowhere to request copy to
                        continue

                    if self.write_rrds:
                        still_missing[dataset.name] = dataset.size
                    else:
                        site_candidates = list(site_candidates)
                        random.shuffle(site_candidates)

                        request_sites = []
                        while num_complete + num_incomplete + len(request_sites) < target_num:
                            # create a request

                            # prioritize the replicas where we can just flip the ownership
                            if len(can_be_flipped) != 0:
                                replica = can_be_flipped.pop()
                                request_sites.append(replica.site)
                                continue

                            if len(site_candidates) == 0:
                                break

                            request_sites.append(site_candidates.pop())

                        for site in request_sites:
                            LOG.debug('Enforcer rule %s requesting %s at %s', rule_name, dataset.name, site.name)
                            product.append((dataset, site))

            if self.write_rrds:
                product.append((rule_name, already_there, en_route, still_missing))

        if not self.write_rrds:
            # product is ordered by site - randomize requests
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
