import time

class InventoryStore(object):
    """
    Interface definition for local inventory data store.
    Implementation of save_* functions must mirror what is in embed_into() of the object.
    Implementation of delete_* functions must mirror what is in unlink_from() of the object.
    """

    def __init__(self, config):
        pass

    def close(self):
        pass

    def check_connection(self):
        """
        Return true if the backend is connected.
        """
        return False

    def get_partition_names(self):
        """
        Return a list of partition names.
        """

        raise NotImplementedError('get_partition_names')

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

    def get_files(self, block):
        """
        Return a set of files belonging to the block.

        @param block  A Block object.
        """
        
        raise NotImplementedError('get_files')

    def load_data(self, inventory, group_names = None, site_names = None, dataset_names = None):
        """
        Load data into inventory.
        
        @param inventory      DynamoInventory object to load data into.
        @param group_names    List of group names to be considered.
        @param site_names     List of site names to be considered.
        @param dataset_names  List of dataset names to be considered.
        """

        raise NotImplementedError('load_data')

    def save_data(self, inventory):
        """
        Save data from inventory.
        
        @param inventory      DynamoInventory object to read data from.
        """

        raise NotImplementedError('save_data')        

    def save_block(self, block):
        raise NotImplementedError('save_block')

    def save_blockreplica(self, block_replica):
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
        """
        If a new partition, create site partitions with default parameters.
        """
        raise NotImplementedError('save_file')

    def save_site(self, site):
        """
        If a new site, create site partitions with default parameters.
        """
        raise NotImplementedError('save_site')

    def save_sitepartition(self, site_partition):
        """
        Should only do updates.
        """
        raise NotImplementedError('save_sitepartition')

    def delete_block(self, block):
        """
        1. Delete all replicas of the block.
        2. Delete all files belonging to the block.
        3. Delete the block.
        """
        raise NotImplementedError('delete_block')

    def delete_blockreplica(self, block_replica):
        """
        1. Delete the block replica.
        2. Delete the owning dataset replica if it becomes empty.
        """
        raise NotImplementedError('delete_blockreplica')

    def delete_dataset(self, dataset):
        """
        1. Delete all replicas of the dataset.
        2. Delete all blocks belonging to the block.
        3. Delete the dataset.
        """
        raise NotImplementedError('delete_dataset')

    def delete_datasetreplica(self, dataset_replica):
        """
        1. Delete all block replicas.
        2. Delete the dataset replica.
        """
        raise NotImplementedError('delete_datasetreplica')

    def delete_group(self, group):
        """
        1. Set owner of all the block replicas owned by the group to None.
        2. Delete the group.
        """
        raise NotImplementedError('delete_group')

    def delete_file(self, lfile):
        """
        1. Delete the file.
        """
        raise NotImplementedError('delete_file')
    
    def delete_partition(self, partition):
        """
        1. Delete all site partitions.
        2. Delete the partition.
        """
        raise NotImplementedError('delete_file')

    def delete_site(self, site):
        """
        1. Delete all dataset replicas at the site.
        2. Delete the site partitions.
        3. Delete the site.
        """
        raise NotImplementedError('delete_site')
