import time
import logging

from common.interface.classes import default_interface
from common.dataformat import DatasetDemand
import common.configuration as config

logger = logging.getLogger(__name__)

class DemandManager(object):
    """
    Aggregate information from multiple sources and create a Demand object for a dataset.
    """

    def __init__(self, store_cls = None, access_history_cls = None, lock_cls = None):
        if store_cls:
            self.store = store_cls()
        else:
            self.store = default_interface['store']()
        
        if access_history_cls:
            self.access_history = access_history_cls()
        else:
            self.access_history = default_interface['access_history']()

        if lock_cls:
            self.lock = lock_cls()
        else:
            self.lock = default_interface['lock']()

        self._last_access_update = 0

    def load(self, inventory):
        sites = inventory.sites.values()
        groups = inventory.groups.values()
        datasets = inventory.datasets.values()

        self._last_access_update = self.store.load_replica_accesses(sites, datasets)
        self.store.load_locks(sites, groups, datasets)

    def update(self, inventory):
        if self._last_access_update == 0:
            logger.info('dataset access ')
            self.load(inventory)

        now = time.mktime(time.gmtime())

        for site in inventory.sites.values():
            time_start = max(self._last_access_update, now - config.demand.access_history.max_query_len) # do not go back more than max_query_len
            while time_start < now - config.demand.access_history.increment:
                self.access_history.set_access_history(site, time_start, time_start + config.demand.access_history.increment)
                time_start += config.demand.access_history.increment

        self._last_access_update = now - config.demand.access_history.increment

        all_replicas = sum([d.replicas for d in inventory.datasets.values()], [])
        self.store.save_replica_accesses(all_replicas)

    def get_demand(self, dataset):
        return DatasetDemand(dataset)


if __name__ == '__main__':

    from argparse import ArgumentParser
    from common.inventory import InventoryManager
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Demand manager')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update)')
    parser.add_argument('--store', '-i', metavar = 'CLASS', dest = 'store_cls', default = '', help = 'Store class to be used.')
    parser.add_argument('--access-history', '-a', metavar = 'CLASS', dest = 'access_history_cls', default = '', help = 'AccessHistory class to be used.')
    parser.add_argument('--lock', '-k', metavar = 'CLASS', dest = 'lock_cls', default = '', help = 'Lock class to be used.')
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

    kwd = {} # not loading data by default to speed up update process

    for cls in ['store', 'access_history', 'lock']:
        clsname = getattr(args, cls + '_cls')
        if clsname == '':
            kwd[cls + '_cls'] = classes.default_interface[cls]
        else:
            kwd[cls + '_cls'] = getattr(classes, clsname)

    manager = DemandManager(**kwd)

    inventory = InventoryManager(load_data = True, store_cls = kwd['store_cls'])

    if command == 'update':
        manager.update(inventory)
