class ProtectedSiteTagger(object):
    """
    Checks if the dataset has a replica on specific sites.
    Sets one attr:
      on_protected_site:    bool
    """

    produces = ['on_protected_site']

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
