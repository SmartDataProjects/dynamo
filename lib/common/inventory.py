from common.interface.classes import default_interface
from common.dataformat import IntegrityError
import common.configuration as config

class InventoryManager(object):
    """Bookkeeping class to bridge the communication between remote and local data sources."""

    def __init__(self, load_data = True, inventory_cls = default_interface['inventory'], data_source_cls = default_interface['status_probe']):
        self.inventory = inventory_cls()
        self.data_source = data_source_cls()

        self.sites = {}
        self.datasets = {}

        if load_data:
            self.load()

    def load(self):
        self.inventory.acquire_lock()

        try:
            sites, datasets = self.inventory.load_data()

            self.sites = sites
            self.datasets = datasets

        finally:
            self.inventory.release_lock()

    def update(self, site_filter = '', dataset_filter = '/*/*/*'):
        """Query the dataSource and get updated information."""

        # Lock the inventory
        self.inventory.acquire_lock()

        try:
            # We start fresh and write all replica information in, instead of updating them.
            # Site, dataset, and block information are kept.
            self.inventory.make_snapshot()

            sites, datasets = self.data_source.get_data(site = site_filter, dataset = dataset_filter)

            self.sites = sites
            self.datasets = datasets

            # Update operation
            # delete site info that is not in the persistent list (nothing happens)
            # create new site info
            # update existing site info (nothing happens)
            self.inventory.save_data(sites, datasets)

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
    parser.add_argument('--site', '-s', metavar = 'EXPR', dest = 'site', default = '', help = 'Limit operation to sites matching the expression.')
    parser.add_argument('--debug', '-v', metavar = 'LEVEL', dest = 'debug_level', type = int, default = 0, help = 'Limit operation to sites matching the expression.')

    args = parser.parse_args()

    config.debug_level = args.debug_level

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
        manager.update(site_filter = args.site, dataset_filter = args.dataset)

    elif command == 'list':
        manager.load()

        target = cmd_args[0]

        if target == 'datasets':
            print manager.inventory.datasets.keys()

        elif target == 'sites':
            print manager.inventory.sites.keys()
