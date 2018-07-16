from dynamo.dataformat import Configuration, ConfigurationError

class ProtectedSiteTagger(object):
    """
    Checks if the dataset has a replica on specific sites.
    Sets one attr:
      on_protected_site:    bool
    """

    _default_config = None

    @staticmethod
    def set_default(config):
        ProtectedSiteTagger._default_config = Configuration(config)

    produces = ['on_protected_site']

    def __init__(self, config = None):
        if config is None:
            if ProtectedSiteTagger._default_config is None:
                raise ConfigurationError('ProtectedSiteTagger default configuration is not set')

            config = ProtectedSiteTagger._default_config

        self.sites = list(config.sites)

    def load(self, inventory):
        if len(self.sites) == 0:
            return

        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                if replica.site.name in self.sites:
                    dataset.attr['on_protected_site'] = True
                    break
