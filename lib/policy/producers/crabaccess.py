import time
import datetime
import collections
import logging
import fnmatch
import MySQLdb

from dynamo.utils.interface.popdb import PopDB
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Site
from dynamo.utils.parallel import Map

LOG = logging.getLogger(__name__)

class CRABAccessHistory(object):
    """
    Sets two attrs:
      global_usage_rank:  float value
      num_access: integer
    """

    produces = ['global_usage_rank', 'num_access']

    def __init__(self, config):
        self._store = MySQL(config.store.db_params)

    def load(self, inventory):
        records = self._get_stored_records(inventory)
        self._compute(inventory, records)

    def _get_stored_records(self, inventory):
        """
        Get the replica access data from DB.
        @param inventory  DynamoInventory
        @return  {dataset: [(date, number of access)]}
        """

        # pick up all accesses that are less than 2 years old
        # old accesses will be removed automatically next time the access information is saved from memory
        sql = 'SELECT d.`name`, UNIX_TIMESTAMP(a.`date`), a.`num_accesses` FROM `dataset_accesses` AS a'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = a.`dataset_id`'
        sql += ' WHERE a.`date` > DATE_SUB(NOW(), INTERVAL 2 YEAR) ORDER BY d.`id`, a.`date`'

        all_accesses = {}
        num_records = 0

        # little speedup by not repeating lookups for the same replica
        current_dataset_name = ''
        dataset_exists = True
        replica = None
        for dataset_name, timestamp, num_accesses in self._store.xquery(sql):
            num_records += 1

            if dataset_name == current_dataset_name:
                if not dataset_exists:
                    continue
            else:
                current_dataset_name = dataset_name

                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    dataset_exists = False
                    continue
                else:
                    dataset_exists = True

                accesses = all_accesses[dataset] = []

            accesses.append((timestamp, num_accesses))

        last_update = self._store.query('SELECT UNIX_TIMESTAMP(`dataset_accesses_last_update`) FROM `system`')[0]

        LOG.info('Loaded %d replica access data. Last update on %s UTC', num_records, time.strftime('%Y-%m-%d', time.gmtime(last_update)))

        return all_accesses

    def _compute(self, inventory, all_accesses):
        """
        Set the dataset usage rank based on access list.
        nAccessed is NACC normalized by size (in GB).
        @param inventory   DynamoInventory
        @param all_accesses {dataset: [(date, number of access)]} (time ordered)
        """

        now = time.time()
        today = datetime.datetime.utcfromtimestamp(now).date()

        for dataset in inventory.datasets.itervalues():
            last_access = 0
            num_access = 0
            norm_access = 0.

            try:
                accesses = all_accesses[dataset]
            except KeyError:
                pass
            else:
                last_access = accesses[-1][0]
                num_access = sum(e[1] for e in accesses)
                if dataset.size != 0:
                    norm_access = float(num_access) / dataset.size

            try:
                last_block_created = max(r.last_block_created() for r in dataset.replicas)
            except ValueError: # empty sequence
                last_block_created = 0

            last_change = max(last_access, dataset.last_update, last_block_created)

            rank = (now - last_change) / (24. * 3600.) - norm_access

            dataset.attr['global_usage_rank'] = rank
            dataset.attr['num_access'] = num_access

    @staticmethod
    def update(config, inventory):
        popdb = PopDB(config.popdb.config)
        store = MySQL(config.store.db_params)

        last_update = store.query('SELECT UNIX_TIMESTAMP(`dataset_accesses_last_update`) FROM `system`')[0]
        try:
            store.query('UPDATE `system` SET `dataset_accesses_last_update` = NOW()', retries = 0, silent = True)
        except MySQLdb.OperationalError:
            # We have a read-only config
            read_only = True
            LOG.info('Running update() in read-only mode.')
        else:
            read_only = False

        start_time = max(last_update, (time.time() - 3600 * 24 * config.max_back_query))
        start_date = datetime.date(*time.gmtime(start_time)[:3])

        included_sites = list(config.included_sites)
        excluded_sites = list(config.excluded_sites)

        source_records = CRABAccessHistory._get_source_records(popdb, inventory, included_sites, excluded_sites, start_date)

        if not read_only:
            CRABAccessHistory._save_records(source_records, store)
            # remove old entries
            store.query('DELETE FROM `dataset_accesses` WHERE `date` < DATE_SUB(NOW(), INTERVAL 2 YEAR)')
            store.query('UPDATE `system` SET `dataset_accesses_last_update` = NOW()')

    @staticmethod
    def _get_source_records(popdb, inventory, included_sites, excluded_sites, start_date):
        """
        Get the replica access data from PopDB from start_date to today.
        @param popdb          PopDB interface
        @param inventory      DynamoInventory
        @param included_sites List of site name patterns to include
        @param excluded_sites List of site name patterns to exclude
        @param start_date     Query start date (datetime.datetime)
        @return  {replica: {date: (number of access, total cpu time)}}
        """

        days_to_query = []

        utctoday = datetime.date(*time.gmtime()[:3])
        date = start_date
        while date <= utctoday: # get records up to today
            days_to_query.append(date)
            date += datetime.timedelta(1) # one day

        LOG.info('Updating dataset access info from %s to %s', start_date.strftime('%Y-%m-%d'), utctoday.strftime('%Y-%m-%d'))

        all_accesses = {}

        arg_pool = []
        for site in inventory.sites.itervalues():
            matched = False
            for pattern in included_sites:
                if fnmatch.fnmatch(site.name, pattern):
                    matched = True
                    break
            for pattern in excluded_sites:
                if fnmatch.fnmatch(site.name, pattern):
                    matched = False
                    break

            if matched:
                for date in days_to_query:
                    arg_pool.append((popdb, site, inventory, date))

        mapper = Map()
        mapper.logger = LOG

        records = mapper.execute(CRABAccessHistory._get_site_record, arg_pool)

        for site_record in records:
            for replica, date, naccess, cputime in site_record:
                if replica not in all_accesses:
                    all_accesses[replica] = {}

                all_accesses[replica][date] = (naccess, cputime)

        return all_accesses

    @staticmethod
    def _get_site_record(popdb, site, inventory, date):
        """
        Get the replica access data on a single site from PopDB.
        @param popdb      PopDB interface
        @param site       Site
        @param inventory  Inventory
        @param date       datetime.date
        @return [(replica, number of access, total cpu time)]
        """

        if site.name.startswith('T0'):
            return []
        elif site.name.startswith('T1') and site.name.count('_') > 2:
            nameparts = site.name.split('_')
            sitename = '_'.join(nameparts[:3])
            service = 'popularity/DSStatInTimeWindow/' # the trailing slash is apparently important
        elif site.name == 'T2_CH_CERN':
            sitename = site.name
            service = 'xrdpopularity/DSStatInTimeWindow'
        else:
            sitename = site.name
            service = 'popularity/DSStatInTimeWindow/'

        datestr = date.strftime('%Y-%m-%d')
        result = popdb.make_request(service, ['sitename=' + sitename, 'tstart=' + datestr, 'tstop=' + datestr])

        records = []
        
        for ds_entry in result:
            try:
                dataset = inventory.datasets[ds_entry['COLLNAME']]
            except KeyError:
                continue

            replica = site.find_dataset_replica(dataset)
            if replica is None:
                continue

            records.append((replica, date, int(ds_entry['NACC']), float(ds_entry['TOTCPU'])))

        return records

    @staticmethod
    def _save_records(records, store):
        """
        Save the newly fetched access records.
        @param records  {replica: {date: (number of access, total cpu time)}}
        @param store    Write-allowed MySQL interface
        """

        site_id_map = {}
        store.make_map('sites', set(r.site for r in records.iterkeys()), site_id_map, None)
        dataset_id_map = {}
        store.make_map('datasets', set(r.dataset for r in records.iterkeys()), dataset_id_map, None)

        fields = ('dataset_id', 'site_id', 'date', 'access_type', 'num_accesses', 'cputime')

        data = []
        for replica, entries in records.iteritems():
            dataset_id = dataset_id_map[replica.dataset]
            site_id = site_id_map[replica.site]

            for date, (num_accesses, cputime) in entries.iteritems():
                data.append((dataset_id, site_id, date.strftime('%Y-%m-%d'), 'local', num_accesses, cputime))

        store.insert_many('dataset_accesses', fields, None, data, do_update = True)
