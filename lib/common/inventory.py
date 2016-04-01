import logging

from common.interface.classes import default_interface
from common.interface.inventory import InventoryInterface
from common.dataformat import IntegrityError, DatasetReplica, BlockReplica
import common.configuration as config

logger = logging.getLogger(__name__)

class ConsistencyError(Exception):
    """Exception to be raised in case of data consistency problem."""
    
    pass


class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self, load_data = False, inventory_cls = None, site_source_cls = None, dataset_source_cls = None, replica_source_cls = None):
        if inventory_cls:
            self.inventory = inventory_cls()
        else:
            self.inventory = default_interface['inventory']()

        if site_source_cls:
            self.site_source = site_source_cls()
        else:
            self.site_source = default_interface['site_source']()

        if dataset_source_cls:
            self.dataset_source = dataset_source_cls()
        else:
            self.dataset_source = default_interface['dataset_source']()

        if replica_source_cls:
            self.replica_source = replica_source_cls()
        else:
            self.replica_source = default_interface['replica_source']()

        self.sites = {}
        self.groups = {}
        self.datasets = {}

        if load_data:
            self.load()

    def load(self, load_replicas = True):
        """
        Load all information from local persistent storage to memory. The flag load_replicas
        can be used to determine whether dataset/block-site links should also be loaded;
        it is set to false when loading for an inventory update (link information is volatile).
        """

        logger.info('Loading data from local persistent storage.')
        
        self.inventory.acquire_lock()

        try:
            sites, groups, datasets = self.inventory.load_data(load_replicas = load_replicas)

            self.sites = dict([(s.name, s) for s in sites])
            self.groups = dict([(g.name, g) for g in groups])
            self.datasets = dict([(d.name, d) for d in datasets])

        finally:
            self.inventory.release_lock()

        logger.info('Data is loaded to memory.')

    def update(self, dataset_filter = '/*/*/*', load_first = True, make_snapshot = True, clean_stale = True):
        """Query the dataSource and get updated information."""

        logger.info('Locking inventory.')

        # Lock the inventory
        self.inventory.acquire_lock()

        try:
            if make_snapshot:
                logger.info('Making a snapshot of inventory.')
                # Make a snapshot (older snapshots cleaned by an independent daemon)
                # All replica data will be erased but the static data (sites, groups, software versions, datasets, and blocks) remain
                self.inventory.make_snapshot(clear = InventoryInterface.CLEAR_REPLICAS)

            if load_first:
                logger.info('Loading existing data.')
                self.load(load_replicas = False)

            logger.info('Fetching info on sites.')
            self.site_source.get_site_list(self.sites, filt = config.inventory.included_sites)

            logger.info('Fetching info on groups.')
            self.site_source.get_group_list(self.groups, filt = config.inventory.included_groups)

            if dataset_filter == '/*/*/*':
                # First construct a full list of dataset names we consider, then make a mass query to optimize speed
                dataset_names = []
                site_count = 0
                for site in self.sites.values():
                    site_count += 1
                    logger.info('Fetching names of datasets on %s (%d/%d).', site.name, site_count, len(self.sites))
    
                    for ds_name in self.replica_source.get_dataset_names(sites = [site], groups = self.groups):
                        if ds_name not in dataset_names:
                            dataset_names.append(ds_name)

            else:
                logger.info('Fetching names of datasets on all sites.')
                dataset_names = self.replica_source.get_dataset_names(sites = self.sites.values(), groups = self.groups, filt = dataset_filter)

            # Do not consider datasets loaded from the inventory but is not on any of the sites
            loaded_datasets = self.datasets.keys()
            for ds_name in loaded_datasets:
                if ds_name not in dataset_names:
                    self.datasets.pop(ds_name)

            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('dataset_names: %s', ' '.join(dataset_names))

            if len(dataset_names) != 0: # should be true for any normal operation. Relevant when debugging
                logger.info('Filling details of %d datasets.', len(self.datasets))

                self.dataset_source.get_datasets(dataset_names, self.datasets)
                self.replica_source.find_tape_copies(self.datasets)
                self.replica_source.make_replica_links(self.sites, self.groups, self.datasets)

            logger.info('Saving data.')
            # Save inventory data to persistent storage
            # Datasets and groups with no replicas are removed
            # Returns the list of newly inserted sites, groups, datasets
            self.inventory.save_data(self.sites, self.groups, self.datasets, clean_stale = clean_stale)

        finally:
            # Lock is released even in case of unexpected errors
            self.inventory.release_lock(force = True)

    def unlink_datasetreplica(self, replica):
        """
        Remove link from datasets and sites to the replica. Don't remove the replica-to-dataset/site link;
        replica objects may be still being used in the program.
        """

        dataset = replica.dataset
        site = replica.site

        # Remove block replicas from the site
        for block_replica in replica.block_replicas:
            block = block_replica.block
            try:
                block.replicas.remove(block_replica)
                site.blocks.remove(block)
                site.used_total -= block.size
                if block_replica.group:
                    try:
                        site.group_usage[block_replica.group] -= block.size
                    except:
                        logger.error('Block %s#%s size was not accounted for group #s', dataset.name, block.name, group.name)

            except ValueError:
                logger.error('Site-block linking was corrupt. %s %s#%s', site.name, dataset.name, block.name)

        try:
            site.datasets.remove(dataset)
            dataset.replicas.remove(replica)
        except ValueError:
            logger.error('Site-dataset linking was corrupt. %s %s', site.name, dataset.name)

    def find_data(self):
        """Query the local DB for datasets/blocks."""
        pass

    def commit(self):
        """
        Commit the updates into the local DB. Might not be necessary
        if diff information is not needed.
        """
        pass


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Inventory manager')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update|list (datasets|sites))')
    parser.add_argument('--inventory', '-i', metavar = 'CLASS', dest = 'inventory_cls', default = '', help = 'Inventory class to be used.')
    parser.add_argument('--site-source', '-s', metavar = 'CLASS', dest = 'site_source_cls', default = '', help = 'SiteInfoSourceInterface class to be used.')
    parser.add_argument('--dataset-source', '-t', metavar = 'CLASS', dest = 'dataset_source_cls', default = '', help = 'DatasetInfoSourceInterface class to be used.')
    parser.add_argument('--replica-source', '-r', metavar = 'CLASS', dest = 'replica_source_cls', default = '', help = 'ReplicaInfoSourceInterface class to be used.')
    parser.add_argument('--dataset', '-d', metavar = 'EXPR', dest = 'dataset', default = '/*/*/*', help = 'Limit operation to datasets matching the expression.')
    parser.add_argument('--no-load', '-L', action = 'store_true', dest = 'no_load',  help = 'Do not load the existing inventory when updating.')
    parser.add_argument('--no-snapshot', '-S', action = 'store_true', dest = 'no_snapshot',  help = 'Do not make a snapshot of existing inventory when updating.')
    parser.add_argument('--no-clean', '-C', action = 'store_true', dest = 'no_clean', help = 'Do not clean up inventory.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    command = args.command[0]
    cmd_args = args.command[1:]

    kwd = {}
    for cls in ['inventory', 'site_source', 'dataset_source', 'replica_source']:
        clsname = getattr(args, cls + '_cls')
        if clsname == '':
            kwd[cls + '_cls'] = classes.default_interface[cls]
        else:
            kwd[cls + '_cls'] = getattr(classes, clsname)

    manager = InventoryManager(**kwd)

    if command == 'update':
        manager.update(dataset_filter = args.dataset, load_first = not args.no_load, make_snapshot = not args.no_snapshot, clean_stale = not args.no_clean)

    elif command == 'list':
        manager.load()

        target = cmd_args[0]

        if target == 'datasets':
            print manager.inventory.datasets.keys()

        elif target == 'sites':
            print manager.inventory.sites.keys()
