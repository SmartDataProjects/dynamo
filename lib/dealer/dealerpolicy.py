import logging
import collections
import random
import fnmatch
import time

LOG = logging.getLogger(__name__)

def target_site_def(site):
    matches = False
    for pattern in dealer_config.main.target_sites:
        if pattern.startswith('!'):
            if fnmatch.fnmatch(site.name, pattern[1:]):
                matches = False
        else:
            if fnmatch.fnmatch(site.name, pattern):
                matches = True

    return matches


class ReplicaPlacementRule(object):
    """
    Defining the interface for replica placement rules.
    """

    def __init__(self):
        pass

    def dataset_allowed(self, dataset, site):
        return True

    def block_allowed(self, block, site):
        return True


class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, partition_name, group = None, version = ''):
        self.partition_name = partition_name
        self.group = group
        self.version = version
        # target site can change between policies, but we only have one policy running at the moment
        self.target_site_def = target_site_def

        self.placement_rules = []
        
    def is_allowed_destination(self, item, site):
        """
        Check if the item (= Dataset, Block, or [Block]) is allowed to be at site, according to the set of rules.
        """

        for rule in self.placement_rules:
            if type(item).__name__ == 'Dataset':
                if not rule.dataset_allowed(item, site):
                    return False

            elif type(item).__name__ == 'Block':
                if not rule.block_allowed(item, site):
                    return False

            elif type(item) is list:
                for block in item:
                    if not rule.block_allowed(block, site):
                        return False

        return True
