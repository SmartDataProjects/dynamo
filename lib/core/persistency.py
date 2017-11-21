import time

class InventoryStore(object):
    """Interface definition for local inventory data store."""

    def __init__(self, config):
        pass

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
        
        @param inventory      DynamoInventory object to load data into.
        @param group_names    List of group names to be considered.
        @param site_names     List of site names to be considered.
        @param dataset_names  List of dataset names to be considered.
        """

        raise NotImplementedError('load_data')

    def save_block(self, block):
        raise NotImplementedError('save_block')

    def save_blockreplica(self, block_replica
        raise NotImplementedError('save_blockreplica')

    def save_dataset(self, dataset):
        raise NotImplementedError('save_dataset')

    def save_datasetreplica(self, dataset_replica):
        raise NotImplementedError('save_datasetreplica')

    def save_group(self, group):
        raise NotImplementedError('save_group')

    def save_file(self, lfile):
        raise NotImplementedError('save_file')
    
    def save_partition(self, partition):
        raise NotImplementedError('save_file')

    def save_site(self, site):
        raise NotImplementedError('save_site')

    def save_sitepartition(self, site_partition):
        raise NotImplementedError('save_sitepartition')

    def delete_block(self, block):
        raise NotImplementedError('delete_block')

    def delete_blockreplica(self, block_replica
        raise NotImplementedError('delete_blockreplica')

    def delete_dataset(self, dataset):
        raise NotImplementedError('delete_dataset')

    def delete_datasetreplica(self, dataset_replica):
        raise NotImplementedError('delete_datasetreplica')

    def delete_group(self, group):
        raise NotImplementedError('delete_group')

    def delete_file(self, lfile):
        raise NotImplementedError('delete_file')
    
    def delete_partition(self, partition):
        raise NotImplementedError('delete_file')

    def delete_site(self, site):
        raise NotImplementedError('delete_site')

    def delete_sitepartition(self, site_partition):
        raise NotImplementedError('delete_sitepartition')
