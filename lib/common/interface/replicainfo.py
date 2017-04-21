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

    def make_replica_links(self, inventory, site_filt = '*', group_filt = '*', dataset_filt = '/*/*/*'):
        """
        Create replica objects and update the site and dataset objects.
        Objects in sites and datasets should have replica information cleared.

        @param inventory    InventoryManager instance
        @param site_filt    Limit to replicas on sites matching the pattern.
        @param group_filt   Limit to replicas owned by groups matching the pattern.
        @param dataset_filt Limit to replicas of datasets matching the pattern.
        """
        pass

class ReplicaInfoSourceDirect(ReplicaInfoSourceInterface):
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

    def make_replica_links(self, sites, groups, datasets):
        """
        Link the sites with datasets and blocks.
        Arguments are name->obj maps
        """
        print "check if I run"
        print sites
        print groups
        print datasets
        

        pass
