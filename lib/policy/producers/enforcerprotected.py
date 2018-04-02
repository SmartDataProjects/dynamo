import collections

from dynamo.dataformat import Configuration
from dynamo.enforcer.interface import EnforcerInterface

class EnforcedProtectionTagger(object):
    """
    Checks if the enforcer rules are respected.
    Sets one attr:
      enforcer_protected_replicas:    set of replicas
    """

    produces = ['enforcer_protected_replicas']

    def __init__(self, config):
        self.partition_name = config.partition
        self.enforcer = EnforcerInterface(config.enforcer)

    def load(self, inventory):
        partition = inventory.partitions[self.partition_name]

        for rule_name, rule in self.enforcer.rules.iteritems():
            target_replicas = collections.defaultdict(set) # {dataset: set(replicas)}
            target_sites = self.enforcer.get_destination_sites(rule_name, inventory, partition)

            for site in target_sites:
                site_partition = site.partitions[partition]

                for replica in site_partition.replicas.iterkeys():
                    for condition in rule.target_replicas:
                        if condition.match(replica):
                            break
                    else:
                        # no condition matched
                        continue

                    target_replicas[replica.dataset].add(replica)

            for dataset, replicas in target_replicas.iteritems():
                if len(replicas) <= rule.num_copies:
                    try:
                        dataset.attr['enforcer_protected_replicas'].update(replicas)
                    except KeyError:
                        dataset.attr['enforcer_protected_replicas'] = replicas
