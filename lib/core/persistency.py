import time

class InventoryStore(object):
    """Interface definition for local inventory data store."""

    def __init__(self, config):
        pass

    def get_last_update(self):
        """Get the last update UNIX timestamp of the inventory store."""

        raise NotImplementedError('get_last_update')

    def set_last_update(self, tm = -1):
        """
        Set the last update UNIX timestamp of the inventory store.

        @param tm  UNIX timestamp of last update. If negative, use time.time().
        """

        raise NotImplementedError('set_last_update')

    def get_group_names(self, include = ['*'], exclude = []):
        """
        Return a list of group full names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the group names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """
        
        raise NotImplementedError('get_group_names')
        
    def get_site_names(self, include = ['*'], exclude = []):
        """
        Return a list of site full names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the site names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """
        
        raise NotImplementedError('get_site_names')

    def get_dataset_names(self, include = ['*'], exclude = []):
        """
        Return a list of dataset names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the dataset names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """
        
        raise NotImplementedError('get_dataset_names')

    def load_data(self, inventory, group_names = None, site_names = None, dataset_names = None):
        """
        Load data into inventory.
        
        @param inventory     DynamoInventory object to load data into.
        @param group_names    List of group names to be considered.
        @param site_names     List of site names to be considered.
        @param dataset_names  List of dataset names to be considered.
        """

        raise NotImplementedError('load_data')
