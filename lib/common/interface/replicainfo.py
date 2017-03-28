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
        Set on_tape properties of datasets with on_tape != TAPE_FULL.
        """
        pass

    def replica_exists_at_site(self, site, item):
        """
        Query individual sites about individual items (dataset, block, or file)
        @param site  Site object
        @param item  Dataset, Block, or File object
        @return Boolean indicating whether a replica exists at the site.
        """

        return False

    def make_replica_links(self, sites, groups, datasets, site_filt = '*', group_filt = '*', dataset_filt = '/*/*/*'):
        """
        Create replica objects and update the site and dataset objects.
        Objects in sites and datasets should have replica information cleared.

        @param sites        {'site_name': site_object}. No new site is created, but the list of replicas of individual sites are updated.
        @param groups       {'group_name': group_object}. Read only.
        @param datasets     {'dataset_name': dataset_object}. New dataset objects are inserted as they are found.
        @param site_filt    Limit to replicas on sites matching the pattern.
        @param group_filt   Limit to replicas owned by groups matching the pattern.
        @param dataset_filt Limit to replicas of datasets matching the pattern.
        """
        pass
