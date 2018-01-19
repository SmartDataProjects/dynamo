import fnmatch

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

    def __init__(self, config, version = ''):
        self.partition_name = config.partition_name
        self.group_name = config.group_name

        self.target_sites = list(config.target_sites)
        # Do not copy data to sites beyond target occupancy fraction (0-1)
        self.target_site_occupancy = config.target_site_occupancy
        # Maximum volume that can be queued for transfer to a single site.
        # The value is given in TB in the configuration file.
        self.max_site_pending_volume = config.max_site_pending_volume * 1.e+12
        # Maximum overall volume that can be queued in this cycle for transfer.
        # The value is given in TB in the configuration file.
        self.max_total_cycle_volume = config.max_total_cycle_volume * 1.e+12

        self.version = version
        self.placement_rules = []

    def target_site_def(self, site):
        matches = False
        for pattern in self.target_sites:
            if pattern.startswith('!'):
                if fnmatch.fnmatch(site.name, pattern[1:]):
                    matches = False
            else:
                if fnmatch.fnmatch(site.name, pattern):
                    matches = True

        return matches
        
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
