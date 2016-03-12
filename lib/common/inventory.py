import logging

from common.interface.classes import default_interface
from common.interface.inventory import InventoryInterface
from common.dataformat import IntegrityError
import common.configuration as config

logger = logging.getLogger(__name__)

class ConsistencyError(Exception):
    """Exception to be raised in case of data consistency problem."""
    
    pass


class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self, load_data = True, inventory_cls = None, site_source_cls = None, dataset_source_cls = None, replica_source_cls = None):
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

    def load(self):
        logger.info('Loading data from local persistent storage.')
        
        self.inventory.acquire_lock()

        try:
            sites, groups, datasets = self.inventory.load_data()

            self.sites = dict([(s.name, s) for s in sites])
            self.groups = dict([(g.name, g) for g in groups])
            self.datasets = dict([(d.name, d) for d in datasets])

        finally:
            self.inventory.release_lock()

        logger.info('Data is loaded to memory.')

    def update(self, dataset_filter = '/*/*/*'):
        """Query the dataSource and get updated information."""

        logger.info('Locking inventory.')

        # Lock the inventory
        self.inventory.acquire_lock()

        try:
            logger.info('Making a snapshot of inventory.')
            # Make a snapshot (older snapshots cleaned by an independent daemon)
            # All replica data will be erased but the static data (sites, groups, datasets, and blocks) remain
            self.inventory.make_snapshot(clear = InventoryInterface.CLEAR_REPLICAS)

            logger.info('Fetching info on sites.')
            site_list = self.site_source.get_site_list(filt = config.inventory.included_sites)
            self.sites = dict([(s.name, s) for s in site_list])

            group_list = self.site_source.get_group_list(filt = config.inventory.included_groups)
            self.groups = dict([(g.name, g) for g in group_list])

            self.datasets = {}

            dataset_names = []
            for site in site_list:
                logger.info('Fetching info on datasets on %s (%d/%d).', site.name, site_list.index(site), len(site_list))

                for ds_name in self.replica_source.get_datasets_on_site(site, dataset_filter):
                    if ds_name not in dataset_names:
                        dataset_names.append(ds_name)

            datasets = []
            if len(dataset_names) != 0: # should be true for any normal operation. Relevant when debugging
                datasets = self.dataset_source.get_datasets(dataset_names)
                self.replica_source.make_replica_links(datasets, self.sites, self.groups)

            self.datasets = dict([(d.name, d) for d in datasets])

            logger.info('Saving data.')
            # Save inventory data to persistent storage
            # Datasets and groups with no replicas are removed
            self.inventory.save_data(self.sites, self.groups, self.datasets)

        finally:
            # Lock is released even in case of unexpected errors
            self.inventory.release_lock(force = True)

    def delete_datasetreplica(self, replica):
        """
        Remove dataset replica info from memory and persistent record.
        """

        dataset = replica.dataset
        site = replica.site

        # Remove block replicas from the site.
        # If no replica is left for the block, delete the block record.
        iB = 0
        while iB < len(dataset.blocks):
            block = dataset.blocks[iB]

            try:
                site.blocks.remove(block)
            except ValueError:
                # this block was not at the site
                pass

            block_repl = block.find_replica(site)
            if block_repl is None:
                raise ConsistencyError('Block %s#%s is registered to %s but no replica object exists.' % (dataset.name, block.name, site.name))

            block.replicas.remove(block_repl)
            self.inventory.delete_blockreplica(block_repl)

            if len(block.replicas) == 0:
                dataset.blocks.remove(block)
                self.inventory.delete_block(block)

            else:
                iB += 1

        # Remove the dataset replica.
        try:
            site.datasets.remove(dataset)
        except ValueError:
            raise ConsistencyError('Dataset %s is registered to %s but site has no block info.' % (dataset.name, site.name))

        dataset.replicas.remove(repl)
        self.inventory.delete_datasetreplica(repl)

        if len(dataset.replicas) == 0:
            self.inventory.delete_dataset(dataset)

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

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--inventory', '-i', metavar = 'CLASS', dest = 'inventory_cls', default = '', help = 'Inventory class to be used.')
    parser.add_argument('--site-source', '-s', metavar = 'CLASS', dest = 'site_source_cls', default = '', help = 'SiteInfoSourceInterface class to be used.')
    parser.add_argument('--dataset-source', '-t', metavar = 'CLASS', dest = 'dataset_source_cls', default = '', help = 'DatasetInfoSourceInterface class to be used.')
    parser.add_argument('--replica-source', '-r', metavar = 'CLASS', dest = 'replica_source_cls', default = '', help = 'ReplicaInfoSourceInterface class to be used.')
    parser.add_argument('--dataset', '-d', metavar = 'EXPR', dest = 'dataset', default = '/*/*/*', help = 'Limit operation to datasets matching the expression.')
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

    manager = InventoryManager(load_data = False, **kwd)

    if command == 'update':
        manager.update(dataset_filter = args.dataset)

    elif command == 'list':
        manager.load()

        target = cmd_args[0]

        if target == 'datasets':
            print manager.inventory.datasets.keys()

        elif target == 'sites':
            print manager.inventory.sites.keys()
