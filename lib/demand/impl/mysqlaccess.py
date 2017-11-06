from common.configuration import common_config
from common.interface.mysql import MySQL

class MySQLAccessHistoryStore(object):
    """
    A persistency class for storing DatasetReplicaUsage information.
    """

    def __init__(self, config):
        db_params = dict(common_config.mysql)
        if 'db_params' in config:
            db_params.update(config['db_params'])

        self._mysql = MySQL(**db_params)

    def load_replica_accesses(self, sites, datasets):
        """
        @param sites    List of sites
        @param datasets List of datasets
        @returns (last update date, {replica: {date: num_access}})
        """

        logger.debug('load_replica_accesses()')

        id_site_map = {}
        self._mysql.make_map('sites', sites, id_object_map = id_site_map)
        id_dataset_map = {}
        self._mysql.make_map('datasets', datasets, id_object_map = id_dataset_map)

        for dataset in datasets:
            if dataset.replicas is None:
                continue

        access_list = {}

        # pick up all accesses that are less than 1 year old
        # old accesses will eb removed automatically next time the access information is saved from memory
        sql = 'SELECT `dataset_id`, `site_id`, YEAR(`date`), MONTH(`date`), DAY(`date`), `access_type`+0, `num_accesses` FROM `dataset_accesses`'
        sql += ' WHERE `date` > DATE_SUB(NOW(), INTERVAL 2 YEAR) ORDER BY `dataset_id`, `site_id`, `date`'

        num_records = 0

        # little speedup by not repeating lookups for the same replica
        current_dataset_id = 0
        current_site_id = 0
        replica = None
        for dataset_id, site_id, year, month, day, access_type, num_accesses in self._mysql.xquery(sql):
            num_records += 1

            if dataset_id != current_dataset_id:
                try:
                    dataset = id_dataset_map[dataset_id]
                except KeyError:
                    continue

                if dataset.replicas is None:
                    continue

                current_dataset_id = dataset_id
                replica = None
                current_site_id = 0

            if site_id != current_site_id:
                try:
                    site = id_site_map[site_id]
                except KeyError:
                    continue

                current_site_id = site_id
                replica = None

            elif replica is None:
                # this dataset-site pair is checked and no replica was found
                continue

            if replica is None:
                replica = dataset.find_replica(site)
                if replica is None:
                    # this dataset is not at the site any more
                    continue

                access_list[replica] = {}

            date = datetime.date(year, month, day)

            access_list[replica][date] = num_accesses

        last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`dataset_accesses_last_update`) FROM `system`')[0]

        logger.info('Loaded %d replica access data. Last update on %s UTC', num_records, time.strftime('%Y-%m-%d', time.gmtime(last_update)))

        return (last_update, access_list)

    def save_replica_accesses(self, access_list):
        """
        Write information in memory into persistent storage.

        @param access_list  {replica: {date: (num_access, cputime)}}
        """

        site_id_map = {}
        self._mysql.make_map('sites', sites, object_id_map = site_id_map)
        dataset_id_map = {}
        self._mysql.make_map('datasets', datasets, object_id_map = dataset_id_map)

        fields = ('dataset_id', 'site_id', 'date', 'access_type', 'num_accesses', 'cputime')

        data = []
        for replica, replica_access_list in access_list.iteritems():
            dataset_id = dataset_id_map[replica.dataset]
            site_id = site_id_map[replica.site]

            for date, (num_accesses, cputime) in replica_access_list.iteritems():
                data.append((dataset_id, site_id, date.strftime('%Y-%m-%d'), 'local', num_accesses, cputime))

        self._mysql.insert_many('dataset_accesses', fields, None, data, do_update = True)

        # remove old entries
        self._mysql.query('DELETE FROM `dataset_accesses` WHERE `date` < DATE_SUB(NOW(), INTERVAL 2 YEAR)')
        self._mysql.query('UPDATE `system` SET `dataset_accesses_last_update` = NOW()')

