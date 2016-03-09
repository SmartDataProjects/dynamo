import logging

from common.interface.classes import default_interface
from common.interface.inventory import InventoryInterface
from common.dataformat import IntegrityError
import common.configuration as config

logger = logging.getLogger(__name__)

class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self, load_data = True, inventory_cls = default_interface['inventory'], data_source_cls = default_interface['status_probe']):
        self.inventory = inventory_cls()
        self.data_source = data_source_cls()

        self.sites = {}
        self.groups = {}
        self.datasets = {}

        if load_data:
            self.load()

    def load(self):
        self.inventory.acquire_lock()

        try:
            sites, groups, datasets = self.inventory.load_data()

            self.sites = sites
            self.groups = groups
            self.datasets = datasets

        finally:
            self.inventory.release_lock()

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

            logger.info('Fetching info on sites, gruops, and datasets.')
            sites, groups, datasets = self.data_source.get_data(site = config.inventory.included_sites, dataset = dataset_filter)

            self.sites = sites
            self.groups = groups
            self.datasets = datasets

            logger.info('Saving data.')
            # Save inventory data to persistent storage
            # Datasets and groups with no replicas are removed
            self.inventory.save_data(sites, groups, datasets)

        finally:
            # Lock is released even in case of unexpected errors
            self.inventory.release_lock(force = True)

    def find_data(self):
        """Query the local DB for datasets/blocks."""
        pass

    def commit(self):
        """Commit the updates into the local DB. Might not be necessary
        if diff information is not needed."""
        pass


if __name__ == '__main__':

    from argparse import ArgumentParser
    from common.interface.classes import *

    parser = ArgumentParser(description = 'Inventory manager')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--inventory', '-i', metavar = 'CLASS', dest = 'inventory_class', default = '', help = 'Inventory class to be used.')
    parser.add_argument('--status-probe', '-p', metavar = 'CLASS', dest = 'status_probe_class', default = '', help = 'Status probe class to be used.')
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

    if args.inventory_class == '':
        inventory_cls = default_interface['inventory']
    else:
        inventory_cls = eval(args.inventory_class)

    if args.status_probe_class == '':
        data_source_cls = default_interface['status_probe']
    else:
        data_source_cls = eval(args.status_probe_class)

    manager = InventoryManager(load_data = False, inventory_cls = inventory_cls, data_source_cls = data_source_cls)

    if command == 'update':
        manager.update(dataset_filter = args.dataset)

    elif command == 'list':
        manager.load()

        target = cmd_args[0]

        if target == 'datasets':
            print manager.inventory.datasets.keys()

        elif target == 'sites':
            print manager.inventory.sites.keys()
