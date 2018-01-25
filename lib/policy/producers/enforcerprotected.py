class EnforcedProtectionTagger(object):
    """
    Checks if the enforcer rules are respected.
    Sets one attr:
      enforcer_protected:    bool
    """

    produces = ['enforcer_protected']

    def __init__(self, config):
        self.sites = list(config.sites)

    def load(self, inventory):
        if len(self.sites) == 0:
            return

        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                if replica.site.name in self.sites:
                    dataset.attr['on_protected_site'] = True
                    break
