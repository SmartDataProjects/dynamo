import re
import fnmatch

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
        for rule_name, rule in self.policy.rules.iteritems():
            site_patterns = []
            for pattern in rule['sites']:
                site_patterns.append(re.compile(fnmatch.translate(pattern)))

            dataset_patterns = []
            for pattern in rule['datasets']:
                dataset_patterns.append(re.compile(fnmatch.translate(pattern)))

            for dataset in inventory.datasets.itervalues():
                for pattern in dataset_patterns:
                    if pattern.match(dataset.name):
                        break
                else:
                    continue

                replicas_in_question = set()
                for replica in dataset.replicas:
                    for pattern in site_patterns:
                        if pattern.match(replica.site.name):
                            replicas_in_question.add(replica)
                            break

                if len(replicas_in_question) <= rule['num_copies']:
                    try:
                        dataset.attr['enforcer_protected_replicas'].update(replicas_in_question)
                    except KeyError:
                        dataset.attr['enforcer_protected_replicas'] = replicas_in_question
