import time
import logging

from dynamo.dataformat import Dataset

LOG = logging.getLogger(__name__)

class InventoryStore(object):
    """
    Interface definition for local inventory data store.
    Implementation of save_* functions must mirror what is in embed_into() of the object.
    Implementation of delete_* functions must mirror what is in unlink_from() of the object.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, InventoryStore):
            raise RuntimeError('%s is not a subclass of InventoryStore' % module)

        return cls(config)

    def __init__(self, config):
        pass

    def close(self):
        pass

    def check_connection(self):
        """
        Return true if the backend is connected.
        """
        return False

    def get_partitions(self, conditions):
        """
        Return a list of partition objects.
        @param conditions  {partition_name: condition} condition can be a Condition object
                           or list of partition names
        """

        raise NotImplementedError('get_partitions')

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
        Save data from inventory. Subclass must implement all _save_X functions. Note that
        the function should be able to accept both an iterable and a generator as their arguments.
        @param inventory      DynamoInventory object to read data from.
        """
        ## Save partitions
        LOG.info('Saving partitions.')

        num = self._save_partitions(inventory.partitions.itervalues())

        LOG.info('Saved %d partitions.', num)

        ## Save groups
        LOG.info('Saving groups.')

        num = self._save_groups(inventory.groups.itervalues())

        LOG.info('Saved %d groups.', num)

        ## Save sites
        LOG.info('Saving sites.')

        num = self._save_sites(inventory.sites.itervalues())

        LOG.info('Saved %d sites.', num)

        ## Save sitepartitions
        LOG.info('Saving sitepartitions.')

        def all_sitepartitions():
            for site in inventory.sites.itervalues():
                for partition in inventory.partitions.itervalues():
                    yield site.partitions[partition]

        num = self._save_sitepartitions(all_sitepartitions())

        LOG.info('Saved %d sitepartitions.', num)

        ## Save datasets
        LOG.info('Saving datasets.')

        num = self._save_datasets(inventory.datasets.itervalues())

        LOG.info('Saved %d datasets.', num)

        ## Save blocks
        LOG.info('Saving blocks.')

        def all_blocks():
            for dataset in inventory.datasets.itervalues():
                for block in dataset.blocks:
                    yield block
        
        num = self._save_blocks(all_blocks())

        LOG.info('Saved %d blocks.', num)

        ## Save files
        LOG.info('Saving files.')

        def all_files():
            for dataset in inventory.datasets.itervalues():
                for block in dataset.blocks:
                    for lfile in block.files:
                        yield lfile

        num = self._save_files(all_files())

        LOG.info('Saved %d files.', num)

        ## Save dataset replicas
        LOG.info('Saving dataset replicas.')

        def all_replicas():
            for site in inventory.sites.itervalues():
                for replica in site.dataset_replicas():
                    yield replica

        num = self._save_dataset_replicas(all_replicas())

        LOG.info('Saved %d dataset replicas.', num)

        ## Save block replicas
        LOG.info('Saving block replicas.')

        def all_replicas():
            for site in inventory.sites.itervalues():
                for dataset_replica in site.dataset_replicas():
                    for block_replica in dataset_replica.block_replicas:
                        yield block_replica

        num = self._save_block_replicas(all_replicas())

        LOG.info('Saved %d block replicas.', num)

    def clone_from(self, source):
        """
        Clone the entire store content from another InventoryStore instance.
        @param source  Source inventory to clone content from.
        """

        if type(source) is type(self):
            # special case using class internals
            self._clone_from_common_class(source)
        else:
            self._clone_from_general(source)

    def _clone_from_general(self, source):
        ## Save partitions
        LOG.info('Saving partitions.')

        num = self._save_partitions(source._yield_partitions())

        LOG.info('Saved %d partitions.', num)

        ## Save groups
        LOG.info('Saving groups.')

        num = self._save_groups(source._yield_groups())

        LOG.info('Saved %d groups.', num)

        ## Save sites
        LOG.info('Saving sites.')

        num = self._save_sites(source._yield_sites())

        LOG.info('Saved %d sites.', num)

        ## Save sitepartitions
        LOG.info('Saving sitepartitions.')

        num = self._save_sitepartitions(source._yield_sitepartitions())

        LOG.info('Saved %d sitepartitions.', num)

        ## Save datasets
        LOG.info('Saving datasets.')

        num = self._save_datasets(source._yield_datasets())

        LOG.info('Saved %d datasets.', num)

        ## Save blocks
        LOG.info('Saving blocks.')
        
        num = self._save_blocks(source._yield_blocks())

        LOG.info('Saved %d blocks.', num)

        ## Save files
        LOG.info('Saving files.')

        num = self._save_files(source._yield_files())

        LOG.info('Saved %d files.', num)

        ## Save dataset replicas
        LOG.info('Saving dataset replicas.')

        num = self._save_dataset_replicas(source._yield_dataset_replicas())

        LOG.info('Saved %d dataset replicas.', num)

        ## Save block replicas
        LOG.info('Saving block replicas.')

        num = self._save_block_replicas(source._yield_block_replicas())

        LOG.info('Saved %d block replicas.', num)

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
