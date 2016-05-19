class ReplicaInfoSourceInterface(object):
    """
    Interface specs for probe to the replica information source.
    """

    def __init__(self):
        pass

    def get_dataset_names(self, sites = [], groups = [], filt = '/*/*/*'):
        """
        Return a list of dataset names on the given site.
        Argument groups is a name->group dict.
        """

        return []

    def find_tape_copies(self, datasets):
        """
        Set on_tape properties of datasets with on_tape = False.
        """
        pass

    def make_replica_links(self, sites, groups, datasets):
        """
        Link the sites with datasets and blocks.
        Arguments are name->obj maps
        """
        pass
