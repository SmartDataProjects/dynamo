class ReplicaInfoSourceInterface(object):
    """
    Interface specs for probe to the replica information source.
    """

    def __init__(self):
        pass

    def find_tape_copies(self, inventory, last_update = 0):
        """
        Set on_tape properties of datasets with on_tape != TAPE_FULL.
        If from_delta is True, not all datasets may be loaded to memory.
        @param inventory    InventoryManager instance
        @param last_update  If set to > 0, delta update from the given unix time is performed.
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

    def make_replica_links(self, inventory, site_filt = '*', group_filt = '*', dataset_filt = '*', last_update = 0):
        """
        Create replica objects and update the site and dataset objects.
        Objects in sites and datasets should have replica information cleared.

        @param inventory    InventoryManager instance
        @param site_filt    Limit to replicas on sites matching the pattern.
        @param group_filt   Limit to replicas owned by groups matching the pattern.
        @param dataset_filt Limit to replicas of datasets matching the pattern.
        @param last_update  If set to > 0, delta update from the given unix time is performed.
        """
        pass
