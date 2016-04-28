import time
import datetime
import logging

from common.interface.classes import default_interface
from common.dataformat import Dataset, DatasetDemand
import common.configuration as config

logger = logging.getLogger(__name__)

class DemandManager(object):
    """
    Aggregate information from multiple sources and create a Demand object for a dataset.
    """

    def __init__(self, store_cls = None, access_history_cls = None, job_queue_cls = None, lock_cls = None):
        if store_cls:
            self.store = store_cls()
        else:
            self.store = default_interface['store']()
        
        if access_history_cls:
            self.access_history = access_history_cls()
        else:
            self.access_history = default_interface['access_history']()

        if job_queue_cls:
            self.job_queue = job_queue_cls()
        else:
            self.job_queue = default_interface['job_queue']()

        if lock_cls:
            self.lock = lock_cls()
        else:
            self.lock = default_interface['lock']()

        self.last_accesses_update = None
        self.last_requests_update = None
        self.time_today = 0.

        self.dataset_demands = {}

    def load(self, inventory):
        logger.info('Loading dataset access information.')

        sites = inventory.sites.values()
        groups = inventory.groups.values()
        datasets = inventory.datasets.values()

        self.last_accesses_update = self.store.load_replica_accesses(sites, datasets)
        self.last_requests_update = self.store.load_dataset_requests(datasets)
        self.store.load_locks(sites, groups, datasets)

        self.setup_demands(inventory)

    def update(self, inventory, accesses = True, requests = True):
        if self.last_accesses_update is None or self.last_requests_update is None:
            self.load(inventory)

        utcnow = datetime.datetime.utcnow()

        utctoday = utcnow.date()

        start_date = max(self.last_accesses_update, utctoday - datetime.timedelta(config.demand.access_history.max_back_query))

        if accesses:
            self.update_accesses(inventory, start_date, utctoday)
            self.last_accesses_update = utctoday - datetime.timedelta(1)

        if requests:
            self.update_requests(inventory, datetime.datetime(start_date.year, start_date.month, start_date.day), utcnow)

        self.setup_demands(inventory)

        utcmidnight = utcnow.replace(hour = 0, minute = 0, second = 0)
        self.time_today = (utcnow - utcmidnight).seconds # n seconds elapsed since UTC 00:00:00 today

    def update_accesses(self, inventory, start_date, end_date):
        """
        Query the access history interface and collect all dataset accesses that happened between
        start date and end date. Save information in the inventory store.
        """

        if self.last_accesses_update is None:
            self.load(inventory)

        for site in inventory.sites.values():
            logger.info('Updating dataset access info at %s from %s to %s', site.name, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

            date = start_date

            while date <= end_date: # get records up to end_date
                self.access_history.set_access_history(site, date)
                date += datetime.timedelta(1) # one day

        all_replicas = []
        for dataset in inventory.datasets.values():
            all_replicas += dataset.replicas

        logger.info('Saving dataset access info for %d replicas.', len(all_replicas))

        self.store.save_replica_accesses(all_replicas)

        if self.last_accesses_update < end_date:
            self.last_accesses_update = end_date

    def update_requests(self, inventory, start_datetime, end_datetime):
        """
        Query the job queue interface and collect all job information between start datetime and
        end datetime. Save information in the inventory store.
        """

        # must convert UTC datetime to UNIX timestamps
        # not the best implementation here but gets the job done
        utc_to_local = datetime.datetime.now() - datetime.datetime.utcnow()
        start_timestamp = time.mktime((start_datetime + utc_to_local).timetuple())
        end_timestamp = time.mktime((end_datetime + utc_to_local).timetuple())
        
        requests = self.job_queue.get_dataset_requests(start_time = start_timestamp, end_time = end_timestamp)

        requested_datasets = []

        for dataset_name, request in requests:
            try:
                dataset = inventory.datasets[dataset_name]
            except KeyError:
                continue

            try:
                req = next(req for req in dataset.requests if req.job_id == request.job_id)
                req.update(request)
            except StopIteration:
                dataset.requests.append(request)

        self.store.save_dataset_requests(inventory.datasets.values())

    def setup_demands(self, inventory):
        self.dataset_demands = {}
        now = time.time()
        
        time_bins = [(now - b[0], b[1]) for b in config.demand.weight_time_bins]

        for dataset in inventory.datasets.values():
            demand = DatasetDemand(required_copies = 1)

            self.dataset_demands[dataset] = demand

            # set the required number of copies using special rules
            for rule in config.demand.required_copies_def:
                r = rule(dataset)
                if r >= 0:
                    demand.required_copies = r
                    break

            if dataset.status != Dataset.STAT_VALID:
                continue

            request_weight = 0.

            for request in dataset.requests:
                if request.queue_time <= time_bins[0][0]:
                    # this request is too old to consider
                    continue

                for ibin in range(len(time_bins) - 1):
                    if request.queue_time < time_bins[ibin + 1][0]:
                        weight = time_bins[ibin][1]
                        break
                else:
                    # in the last bin
                    weight = time_bins[-1][1]

                request_weight += weight

            demand.request_weight = request_weight


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser
    from common.inventory import InventoryManager
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Demand manager')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update [access start end]) [commands]')
    parser.add_argument('--store', '-i', metavar = 'CLASS', dest = 'store_cls', default = '', help = 'Store class to be used.')
    parser.add_argument('--access-history', '-a', metavar = 'CLASS', dest = 'access_history_cls', default = '', help = 'AccessHistory class to be used.')
    parser.add_argument('--lock', '-k', metavar = 'CLASS', dest = 'lock_cls', default = '', help = 'Lock class to be used.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    kwd = {} # not loading data by default to speed up update process

    for cls in ['store', 'access_history', 'lock']:
        clsname = getattr(args, cls + '_cls')
        if clsname == '':
            kwd[cls + '_cls'] = classes.default_interface[cls]
        else:
            kwd[cls + '_cls'] = getattr(classes, clsname)

    manager = DemandManager(**kwd)

    inventory = InventoryManager(load_data = True, store_cls = kwd['store_cls'])

    icmd = 0
    while icmd != len(args.command):
        command = args.command[icmd]
        icmd += 1

        if command == 'update':
            if len(args.command[icmd:]) != 0:
                arg = args.command[icmd]

                manager.load(inventory)

                if arg == 'accesses':
                    icmd += 1

                    start, end = args.command[icmd:icmd + 2]
                    icmd += 2

                    if start == 'last':
                        start_date = manager.last_accesses_update
                    else:
                        start_date = datetime.datetime.strptime(start, '%Y-%m-%d').date()                        

                    if end == 'now':
                        end_date = datetime.date.today()
                    else:
                        end_date = datetime.datetime.strptime(end, '%Y-%m-%d').date()
    
                    manager.update_accesses(inventory, start_date, end_date)
    
                elif arg == 'requests':
                    icmd += 1

                    start, end = args.command[icmd:icmd + 2]
                    icmd += 2

                    if start == 'last':
                        start_datetime = manager.last_requests_update
                    else:
                        start_datetime = datetime.datetime.strptime(start, '%Y-%m-%d')

                    if end == 'now':
                        end_datetime = datetime.datetime.utcnow()
                    else:
                        end_datetime = datetime.datetime.strptime(end, '%Y-%m-%d')
    
                    manager.update_requests(inventory, start_datetime, end_datetime)
    
            else:
                manager.update(inventory)
                icmd += 1
