import re

from dynamo.dataformat import Configuration

class EnforcedProtectionTagger(object):
    """
    Checks if the enforcer rules are respected.
    Sets one attr:
      enforcer_protected_replicas:    set of replicas
    """

    produces = ['enforcer_protected_replicas']

    def __init__(self, config):
        self.policy = Configuration(config.policy)

    def load(self, inventory):
        for rule in self.policy.rules:        
            for dataset in inventory.datasets.itervalues():
                pattern = re.compile(rule['datasets'].replace("*","[^\s]*"))
                if not pattern.match(dataset.name):
                    continue

                replicas_in_question = []

                for sitename in rule['sites']:
                    pattern = re.compile(sitename.replace("*","[^\s]*"))
                    for site in inventory.sites.values():
                        if not pattern.match(site.name):
                            continue
                        if dataset.find_replica(site) is not None:
                            replicas_in_question.append(dataset.find_replica(site))

                if len(replicas_in_question) <= rule['copies']:
                    dataset.attr['enforcer_protected_replicas'] = set(replicas_in_question)
