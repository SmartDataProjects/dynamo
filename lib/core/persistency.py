import logging
import time

from dataformat import Dataset

logger = logging.getLogger(__name__)

class InventoryStore(object):
    """Interface definition for local inventory data store."""

    def __init__(self, config):
        pass

    def get_last_update(self):
        """Get the last update UNIX timestamp of the inventory store."""

        logger.debug('get_last_update()')
        return self._do_get_last_update()

    def set_last_update(self, tm = -1):
        """
        Set the last update UNIX timestamp of the inventory store.

        @param tm  UNIX timestamp of last update. If negative, use time.time().
        """

        logger.debug('set_last_update()')

        if tm < 0:
            tm = time.time()

        self._do_set_last_update(tm)

    def get_site_list(self, include = ['*'], exclude = []):
        """
        Return a list of site full names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the site names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """

        logger.debug('get_site_list()')
        
        return self._do_get_site_list(include, exclude)

    def get_dataset_list(self, include = ['*'], exclude = []):
        """
        Return a list of dataset names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the dataset names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """

        logger.debug('get_dataset_list()')
        
        return self._do_get_dataset_list(include, exclude)

    def load_data(self, inventory, site_filt = '*', dataset_filt = '*', load_blocks = False, load_files = False, load_replicas = True):
        """
        Return lists loaded from persistent storage. Argument site_filt can be a wildcard string or a list
        of exact site names.
        
        @param inventory     DynamoInventory object to load data into.
        @param site_filt     String or list of strings of fnmatch patterns of sites to be considered.
        @param dataest_filt  String or list of strings of fnmatch patterns of datasets to be considered.
        @param load_blocks   If true, load blocks into dataset objects.
        @param load_files    If true, load files into block objects.
        @param load_replicas If true, load dataset and block replicas (require load_blocks = True).
        """

        logger.debug('load_data()')

        self._do_load_data(inventory, site_filt, dataset_filt, load_blocks, load_files, load_replicas)

    def load_dataset(self, inventory, dataset_name, load_blocks = False, load_files = False, load_replicas = False):
        """
        Load a dataset into inventory.

        @param inventory     DynamoInventory object.
        @param dataset_name  Name of the dataset.
        @param load_blocks,load_files,load_replicas  See load_data().
        """

        logger.debug('load_dataset()')

        self._do_load_dataset(inventory, dataset_name, load_blocks, load_files, load_replicas, sites, groups)

    def load_replicas(self, dataset):
        """
        Load replicas for the given dataset.
        """

        logger.debug('load_replicas()')
        
        self._do_load_replicas(dataset, sites, groups)

    def load_blocks(self, dataset):
        """
        Load blocks for the given dataset.
        """

        logger.debug('load_blocks()')

        self._do_load_blocks(dataset)

    def load_files(self, block):
        """
        Load files for the given dataset.
        """

        logger.debug('load_files()')
        
        self._do_load_files(block)

    def find_block_of(self, fullpath, datasets):
        """
        Return the Block object for the given file.
        """

        logger.debug('find_block_of()')

        return self._do_find_block_of(fullpath, datasets)

    def save_data(self, inventory, timestamp = -1, delta = True):
        """
        Write information in memory into persistent storage.
        Remove information of datasets and blocks with no replicas.
        Arguments are list of objects.

        @param inventory  DynamoInventory.
        @param timestamp  Last update timestamp. See set_last_update().
        @param delta      Incrementally update the replicas
        """

        logger.debug('save_data()')

        self._do_save_sites(inventory.sites.itervalues())
        self._do_save_groups(inventory.groups.itervalues())
        self._do_save_datasets(inventory.datasets.itervalues())
        if delta:
            self._do_update_replicas(inventory)
        else:
            self._do_save_replicas(inventory)

        self.set_last_update(timestamp)

    def add_datasetreplicas(self, replicas):
        """
        Insert a few replicas instead of saving the full list.
        """

        self._do_add_datasetreplicas(replicas)

    def add_blockreplicas(self, replicas):
        """
        Insert a few replicas instead of saving the full list.
        """

        self._do_add_blockreplicas(replicas)

    def delete_dataset(self, dataset):
        """
        Delete dataset from persistent storage.
        """

        self._do_delete_dataset(dataset)

    def delete_datasets(self, datasets):
        """
        Delete datasets from persistent storage.
        """

        self._do_delete_datasets(datasets)

    def delete_block(self, block):
        """
        Delete block from persistent storage.
        """

        self._do_delete_block(block)

    def delete_datasetreplica(self, replica, delete_blockreplicas = True):
        """
        Delete dataset replica from persistent storage.
        If delete_blockreplicas is True, delete block replicas associated to this dataset replica too.
        """

        self.delete_datasetreplicas([replica], delete_blockreplicas = delete_blockreplicas)

    def delete_datasetreplicas(self, replica_list, delete_blockreplicas = True):
        """
        Delete a set of dataset replicas from persistent storage.
        If delete_blockreplicas is True, delete block replicas associated to the dataset replicas too.
        """

        sites = list(set([r.site for r in replica_list]))
        datasets_on_site = dict([(site, []) for site in sites])
        
        for replica in replica_list:
            datasets_on_site[replica.site].append(replica.dataset)

        for site in sites:
            self._do_delete_datasetreplicas(site, datasets_on_site[site], delete_blockreplicas)

    def delete_blockreplica(self, replica):
        """
        Delete block replica from persistent storage.
        """

        self.delete_blockreplicas([replica])

    def delete_blockreplicas(self, replica_list):
        """
        Delete a set of block replicas from persistent storage.
        """

        self._do_delete_blockreplicas(replica_list)

    def update_blockreplica(self, replica):
        """
        Update block replica in persistent storage.
        """

        self.update_blockreplicas([replica])

    def update_blockreplicas(self, replica_list):
        """
        Update a set of block replicas in persistent storage.
        """

        self._do_update_blockreplicas(replica_list)

    def set_dataset_status(self, dataset, status):
        """
        Set and save dataset status
        """

        # convert status into a string
        status_str = Dataset.status_name(status)

        if type(dataset) is Dataset:
            dataset_name = dataset.name
        elif type(dataset) is str:
            dataset_name = dataset

        self._do_set_dataset_status(dataset_name, status_str)
