class ReplicaInfoSourceInterface(object):
    """
    Interface specs for probe to the replica information source.
    """

    def __init__(self):
        pass

    def get_datasets_on_site(self, site, filt = '/*/*/*'):
        """Return a list of dataset names on the given site."""

        return []

    def make_replica_links(self, datasets, sites, groups):
        """Link the sites with datasets and blocks"""
        pass
