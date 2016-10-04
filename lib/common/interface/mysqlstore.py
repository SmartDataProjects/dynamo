import os
import time
import datetime
import re
import socket
import logging
import fnmatch

from common.interface.store import LocalStoreInterface
from common.interface.mysql import MySQL
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica, DatasetRequest
import common.configuration as config

logger = logging.getLogger(__name__)

class MySQLStore(LocalStoreInterface):
    """Interface to MySQL."""

    class DatabaseError(Exception):
        pass

    def __init__(self):
        super(self.__class__, self).__init__()

        self._mysql = MySQL(**config.mysqlstore.db_params)

        self.last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0] # MySQL displays last_update in local time, but returns the UTC timestamp

        self._datasets_to_ids = {} # cache dictionary object -> mysql id
        self._sites_to_ids = {} # cache dictionary object -> mysql id
        self._groups_to_ids = {} # cache dictionary object -> mysql id
        self._ids_to_datasets = {} # cache dictionary mysql id -> object
        self._ids_to_sites = {} # cache dictionary mysql id -> object
        self._ids_to_groups = {} # cache dictionary mysql id -> object

    def _do_acquire_lock(self, blocking): #override
        while True:
            # Use the system table to "software-lock" the database
            self._mysql.query('LOCK TABLES `system` WRITE')
            self._mysql.query('UPDATE `system` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', socket.gethostname(), os.getpid())

            # Did the update go through?
            host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `system`')[0]
            self._mysql.query('UNLOCK TABLES')

            if host == socket.gethostname() and pid == os.getpid():
                # The database is locked.
                break

            if blocking:
                logger.warning('Failed to lock database. Waiting 30 seconds..')
                time.sleep(30)
            else:
                logger.warning('Failed to lock database.')
                return False

        return True

    def _do_release_lock(self, force): #override
        self._mysql.query('LOCK TABLES `system` WRITE')
        if force:
            self._mysql.query('UPDATE `system` SET `lock_host` = \'\', `lock_process` = 0')
        else:
            self._mysql.query('UPDATE `system` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `system`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise LocalStoreInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_make_snapshot(self, tag, clear): #override
        new_db = self._mysql.make_snapshot(tag)
        
        self._mysql.query('UPDATE `%s`.`system` SET `lock_host` = \'\', `lock_process` = 0' % new_db)

        tables = []
        if clear == LocalStoreInterface.CLEAR_ALL:
            tables = self._mysql.query('SHOW TABLES')
        elif clear == LocalStoreInterface.CLEAR_REPLICAS:
            tables = ['dataset_replicas', 'block_replicas', 'block_replica_sizes']

        for table in tables:
            if table == 'system':
                continue

            # drop the original table and copy back the format from the snapshot
            self._mysql.query('TRUNCATE TABLE `{orig}`.`{table}`'.format(orig = self._mysql.db_name(), table = table))

    def _do_remove_snapshot(self, tag, newer_than, older_than): #override
        if tag:
            self._mysql.remove_snapshot(tag = tag)
        else:
            self._mysql.remove_snapshot(newer_than = newer_than, older_than = older_than)

    def _do_list_snapshots(self, timestamp_only):
        return self._mysql.list_snapshots(timestamp_only)

    def _do_clear_cache(self): #override
        """
        Clear the id <-> object mappings.
        """

        self._datasets_to_ids = {} # cache dictionary object -> mysql id
        self._sites_to_ids = {} # cache dictionary object -> mysql id
        self._groups_to_ids = {} # cache dictionary object -> mysql id
        self._ids_to_datasets = {} # cache dictionary mysql id -> object
        self._ids_to_sites = {} # cache dictionary mysql id -> object
        self._ids_to_groups = {} # cache dictionary mysql id -> object

    def _do_clear(self):
        tables = self._mysql.query('SHOW TABLES')
        tables.remove('system')

        for table in tables:
            # drop the original table and copy back the format from the snapshot
            self._mysql.query('TRUNCATE TABLE `{orig}`.`{table}`'.format(orig = self._mysql.db_name(), table = table))

    def _do_recover_from(self, tag): #override
        self._mysql.recover_from(tag)

    def _do_switch_snapshot(self, tag): #override
        snapshot_name = self._mysql.db_name() + '_' + tag

        self._mysql.query('USE ' + snapshot_name)

    def _do_set_last_update(self, tm): #override
        self._mysql.query('UPDATE `system` SET `last_update` = FROM_UNIXTIME(%d)' % int(tm))

    def _do_get_site_list(self, site_filt): #override
        # Load sites
        site_names = []

        names = self._mysql.query('SELECT `name` FROM `sites`')

        if type(site_filt) is str:
            site_filt = [site_filt]
        
        for name in names:
            for filt in site_filt:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                continue

            site_names.append(name)
        
        return site_names

    def _do_load_data(self, site_filt, dataset_filt, load_replicas): #override
        if type(site_filt) is list and len(site_filt) == 0:
            return [], [], []

        # Load sites
        site_list = []

        query = 'SELECT `name`, `host`, `storage_type`+0, `backend`, `storage`, `cpu`, `status`+0 FROM `sites`'
        if type(site_filt) is str and site_filt != '*' and site_filt != '':
            query += ' WHERE `name` LIKE \'%s\'' % site_filt.replace('*', '%')
        elif type(site_filt) is list:
            query += ' WHERE `name` IN (%s)' % (','.join('\'%s\'' % s for s in site_filt))

        for name, host, storage_type, backend, storage, cpu, status in self._mysql.query(query):
            site = Site(name, host = host, storage_type = Site.storage_type_val(storage_type), backend = backend, storage = storage, cpu = cpu, status = status)
            site_list.append(site)

        self._set_site_ids(site_list)

        logger.info('Loaded data for %d sites.', len(site_list))

        if len(site_list) == 0:
            return [], [], []

        sites_str = ''
        if site_filt == '*' or site_filt == '':
            sites_str = ','.join(['%d' % i for i in self._ids_to_sites.keys()])

        # Load groups
        group_list = []

        for name, olname in self._mysql.query('SELECT `name`, `olevel` FROM `groups`'):
            if olname == 'Dataset':
                olevel = Dataset
            else:
                olevel = Block

            group = Group(name, olevel)
            group_list.append(group)

        self._set_group_ids(group_list)

        logger.info('Loaded data for %d groups.', len(group_list))

#        # Load site quotas
#        quotas = self._mysql.query('SELECT `site_id`, `group_id`, `storage` FROM `quotas`')
#        for site_id, group_id, storage in quotas:
#            try:
#                site = self._ids_to_sites[site_id]
#            except KeyError:
#                continue
#
#            try:
#                group = self._ids_to_groups[group_id]
#            except KeyError:
#                continue
#
#            site.set_group_quota(group, storage)
#
#        for site in site_list:
#            for group in group_list:
#                if site.group_present(group):
#                    logger.info('Setting quota for %s on %s to %d', group.name, site.name, int(site.storage / len(group_list)))
#                    site.set_group_quota(group, int(site.storage / len(group_list)))

        # Load software versions - treat directly as tuples with id in first column
        software_version_map = {0: None}
        for vtuple in self._mysql.query('SELECT * FROM `software_versions`'):
            software_version_map[vtuple[0]] = vtuple[1:]

        # Load datasets - only load ones with replicas on selected sites
        dataset_list = []

        query = 'SELECT DISTINCT d.`name`, d.`status`+0, d.`on_tape`, d.`data_type`+0, d.`software_version_id`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open`'
        query += ' FROM `datasets` AS d'
        conditions = []
        if load_replicas and sites_str:
            query += ' INNER JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = d.`id`'
            conditions.append('dr.`site_id` IN (%s)' % sites_str)
        if dataset_filt != '/*/*/*' and dataset_filt != '':
            conditions.append('d.`name` LIKE \'%s\'' % dataset_filt.replace('*', '%'))

        if len(conditions) != 0:
            query += ' WHERE ' + (' AND '.join(conditions))

        for name, status, on_tape, data_type, software_version_id, last_update, is_open in self._mysql.query(query):
            dataset = Dataset(name, status = int(status), on_tape = on_tape, data_type = int(data_type), last_update = last_update, is_open = (is_open == 1))
            dataset.software_version = software_version_map[software_version_id]

            dataset_list.append(dataset)

        self._set_dataset_ids(dataset_list)

        logger.info('Loaded data for %d datasets.', len(dataset_list))

        if len(dataset_list) == 0:
            return site_list, group_list, dataset_list

        # Load blocks
        block_id_maps = {} # {dataset_id: {block_id: block}}

        query = 'SELECT DISTINCT b.`id`, b.`dataset_id`, b.`name`, b.`size`, b.`num_files`, b.`is_open` FROM `blocks` AS b'
        query += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        conditions = []
        if load_replicas and sites_str:
            query += ' INNER JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = d.`id`'
            conditions.append('dr.`site_id` IN (%s)' % sites_str)
        if dataset_filt != '/*/*/*' and dataset_filt != '':
            conditions.append('d.`name` LIKE \'%s\'' % dataset_filt.replace('*', '%'))

        if len(conditions) != 0:
            query += ' WHERE ' + (' AND '.join(conditions))

        query += ' ORDER BY b.`dataset_id`'

        num_blocks = 0

        _dataset_id = 0
        dataset = None
        for block_id, dataset_id, name, size, num_files, is_open in self._mysql.query(query):
            if dataset_id != _dataset_id:
                dataset = self._ids_to_datasets[dataset_id]
                block_id_map = {}
                block_id_maps[dataset_id] = block_id_map
                _dataset_id = dataset_id

            block = Block(Block.translate_name(name), dataset, size, num_files, is_open)

            dataset.blocks.append(block)
            block_id_map[block_id] = block

            num_blocks += 1

        logger.info('Loaded data for %d blocks.', num_blocks)

        if load_replicas:
            logger.info('Loading replicas.')

            sql = 'SELECT dr.`dataset_id`, dr.`site_id`, dr.`completion`, dr.`is_custodial`, UNIX_TIMESTAMP(dr.`last_block_created`),'
            sql += ' br.`block_id`, br.`group_id`, br.`is_complete`, br.`is_custodial`, brs.`size`'
            sql += ' FROM `dataset_replicas` AS dr'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`'
            sql += ' INNER JOIN `blocks` AS b ON b.`dataset_id` = d.`id`'
            sql += ' INNER JOIN `block_replicas` AS br ON (br.`block_id`, br.`site_id`) = (b.`id`, dr.`site_id`)'
            sql += ' LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (br.`block_id`, br.`site_id`)'

            conditions = []
            if sites_str:
                conditions.append('dr.`site_id` IN (%s)' % sites_str)
            if dataset_filt != '/*/*/*' and dataset_filt != '':
                conditions.append('dr.`dataset_id` IN (%s)' % (','.join(['%d' % i for i in self._ids_to_datasets.keys()])))

            if len(conditions) != 0:
                sql += ' WHERE ' + (' AND '.join(conditions))

            sql += ' ORDER BY dr.`dataset_id`, dr.`site_id`'

            _dataset_id = 0
            _site_id = 0
            dataset_replica = None
    
            for dataset_id, site_id, completion, is_custodial, last_block_created, block_id, group_id, is_complete, b_is_custodial, b_size in self._mysql.query(sql):
                if dataset_id != _dataset_id:
                    _dataset_id = dataset_id
                    dataset = self._ids_to_datasets[_dataset_id]

                    block_id_map = block_id_maps[dataset_id]

                if site_id != _site_id:
                    _site_id = site_id
                    site = self._ids_to_sites[site_id]

                if dataset_replica is None or dataset != dataset_replica.dataset or site != dataset_replica.site:
                    dataset_replica = DatasetReplica(dataset, site, is_complete = (completion != 'incomplete'), is_custodial = is_custodial, last_block_created = last_block_created)

                    dataset.replicas.append(dataset_replica)
                    site.dataset_replicas.add(dataset_replica)

                block = block_id_map[block_id]

                group = self._ids_to_groups[group_id]

                block_replica = BlockReplica(block, site, group = group, is_complete = is_complete, is_custodial = b_is_custodial, size = block.size if b_size is None else b_size)

                dataset_replica.block_replicas.append(block_replica)
                site.add_block_replica(block_replica)

        # Finally set last_update
        self.last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

        # Only the list of sites, groups, and datasets are returned
        return site_list, group_list, dataset_list

    def _do_load_replica_accesses(self, sites, datasets): #override
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(datasets)
        if len(self._sites_to_ids) == 0:
            self._set_site_ids(sites)

        for dataset in datasets:
            for replica in dataset.replicas:
                replica.accesses[DatasetReplica.ACC_LOCAL].clear()
                replica.accesses[DatasetReplica.ACC_REMOTE].clear()

        # pick up all accesses that are less than 1 year old
        # old accesses will eb removed automatically next time the access information is saved from memory
        accesses = self._mysql.query('SELECT `dataset_id`, `site_id`, YEAR(`date`), MONTH(`date`), DAY(`date`), `access_type`+0, `num_accesses`, `cputime` FROM `dataset_accesses` WHERE `date` > DATE_SUB(NOW(), INTERVAL 2 YEAR) ORDER BY `dataset_id`, `site_id`, `date`')

        # little speedup by not repeating lookups for the same replica
        current_dataset_id = 0
        current_site_id = 0
        replica = None
        for dataset_id, site_id, year, month, day, access_type, num_accesses, cputime in accesses:
            if dataset_id != current_dataset_id:
                try:
                    dataset = self._ids_to_datasets[dataset_id]
                except KeyError:
                    continue

                current_dataset_id = dataset_id
                replica = None
            
            if site_id != current_site_id:
                try:
                    site = self._ids_to_sites[site_id]
                except KeyError:
                    continue

                current_site_id = site_id
                replica = None

            if replica is None:
                replica = dataset.find_replica(site)
                if replica is None:
                    # this dataset is not at the site any more
                    continue

            date = datetime.date(year, month, day)
            replica.accesses[int(access_type)][date] = DatasetReplica.Access(num_accesses, cputime)

        last_update = datetime.datetime.utcfromtimestamp(self._mysql.query('SELECT UNIX_TIMESTAMP(`dataset_accesses_last_update`) FROM `system`')[0])

        logger.info('Loaded %d replica access data. Last update on %s UTC', len(accesses), last_update.strftime('%Y-%m-%d'))

        return last_update.date()

    def _do_load_dataset_requests(self, datasets): #override
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(datasets)

        for dataset in datasets:
            dataset.requests = []

        # pick up requests that are less than 1 year old
        # old requests will be removed automatically next time the access information is saved from memory
        requests = self._mysql.query('SELECT `id`, `dataset_id`, UNIX_TIMESTAMP(`queue_time`), UNIX_TIMESTAMP(`completion_time`), `nodes_total`, `nodes_done`, `nodes_failed`, `nodes_queued` FROM `dataset_requests` WHERE `queue_time` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `queue_time`')

        # little speedup by not repeating lookups for the same dataset
        current_dataset_id = 0
        for job_id, dataset_id, queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued  in requests:
            if dataset_id != current_dataset_id:
                try:
                    dataset = self._ids_to_datasets[dataset_id]
                except KeyError:
                    continue

                current_dataset_id = dataset_id

            request = DatasetRequest(
                job_id = job_id,
                queue_time = queue_time,
                completion_time = completion_time,
                nodes_total = nodes_total,
                nodes_done = nodes_done,
                nodes_failed = nodes_failed,
                nodes_queued = nodes_queued
            )

            dataset.requests.append(request)

            update = datetime.datetime.utcfromtimestamp(completion_time)

        last_update = datetime.datetime.utcfromtimestamp(self._mysql.query('SELECT UNIX_TIMESTAMP(`dataset_requests_last_update`) FROM `system`')[0])

        logger.info('Loaded %d dataset request data. Last update at %s UTC', len(requests), last_update.strftime('%Y-%m-%d %H:%M:%S'))

        return last_update

    def _do_save_sites(self, sites): #override
        # insert/update sites
        logger.info('Inserting/updating %d sites.', len(sites))

        fields = ('name', 'host', 'storage_type', 'backend', 'storage', 'cpu', 'status')
        mapping = lambda s: (s.name, s.host, Site.storage_type_name(s.storage_type), s.backend, s.storage, s.cpu, s.status)

        self._mysql.insert_many('sites', fields, mapping, sites)

        # site_ids map not used here but needs to be reloaded
        self._set_site_ids(sites)

    def _do_save_groups(self, groups): #override
        # insert/update groups
        logger.info('Inserting/updating %d groups.', len(groups))

        self._mysql.insert_many('groups', ('name', 'olevel'), lambda g: (g.name, g.olevel.__name__), groups)

        # group_ids map not used here but needs to be reloaded
        self._set_group_ids(groups)

    def _do_save_datasets(self, datasets): #override
        # insert/update software versions

        version_map = {None: 0} # tuple -> id
        for vtuple in self._mysql.query('SELECT * FROM `software_versions`'):
            version_map[vtuple[1:]] = vtuple[0]

        all_versions = set([d.software_version for d in datasets])
        for v in all_versions:
            if v not in version_map:
                # id = 0 automatically generates the next id
                new_id = self._mysql.query('INSERT INTO `software_versions` VALUES %s' % str((0,) + v))
                version_map[v] = new_id

        # insert/update datasets
        logger.info('Inserting/updating %d datasets.', len(datasets))

        if len(self._datasets_to_ids) == 0:
            # load up the latest dataset ids
            self._set_dataset_ids(datasets)

        # instead of insert + on duplicate update, which will cost N x logN, we will create a temporary table, join, and update (N + N)

        self._mysql.query('CREATE TABLE `datasets_tmp` LIKE `datasets`')

        # sort in memory so MySQL write is most efficient
        tmp_insertions = sorted(self._ids_to_datasets.items())

        # datasets.size stored only for query speedup in inventory web interface
        fields = ('id', 'name', 'size', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda (i, d): (
            i,
            d.name,
            d.size(),
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
            1 if d.is_open else 0
        )
        
        self._mysql.insert_many('datasets_tmp', fields, mapping, tmp_insertions, do_update = False)

        field_tuple = lambda t: '(' + ', '.join(['%s.`%s`' % (t, f) for f in fields[2:]]) + ')'

        query = 'UPDATE `datasets` AS d1 INNER JOIN `datasets_tmp` AS d2 ON d1.`id` = d2.`id` SET'
        query += ' ' + ', '.join(['d1.`{f}` = d2.`{f}`'.format(f = f) for f in fields[2:]])
        query += ' WHERE ' + field_tuple('d1') + ' != ' + field_tuple('d2')

        self._mysql.query(query)

        self._mysql.query('DROP TABLE `datasets_tmp`')

        known_datasets = set([d for i, d in tmp_insertions])
        new_datasets = list(set(datasets) - known_datasets)

        if len(new_datasets) != 0:
            fields = ('name', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
            # MySQL expects the local time for last_update
            mapping = lambda d: (
                d.name,
                d.status,
                d.on_tape,
                d.data_type,
                version_map[d.software_version],
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
                1 if d.is_open else 0
            )
    
            self._mysql.insert_many('datasets', fields, mapping, new_datasets, do_update = False)

            # reload the dataset ids
            self._set_dataset_ids(new_datasets, update = True)

        del tmp_insertions
        del known_datasets
        del new_datasets

        # insert/update blocks

        # same strategy; write to temp and update
        # this time however we don't want to create a block-to-id map
        # which also means that we don't a priori know which blocks are in the DB already
        # we therefore have to dump all blocks into a temporary table

        all_blocks = []
        for dataset in datasets:
            all_blocks.extend(dataset.blocks)

        logger.info('Inserting/updating %d blocks.', len(all_blocks))

        # NOTE: here we rely on the assumption that block names are unique

        self._mysql.query('CREATE TABLE `blocks_tmp` LIKE `blocks`')
        # make block name the primary key
        self._mysql.query('ALTER TABLE `blocks_tmp` DROP COLUMN `id`')

        fields = ('dataset_id', 'name', 'size', 'num_files', 'is_open')
        mapping = lambda b: (
            self._datasets_to_ids[b.dataset],
            b.real_name(),
            b.size,
            b.num_files,
            1 if b.is_open else 0
        )

        self._mysql.insert_many('blocks_tmp', fields, mapping, all_blocks, do_update = False)

        del all_blocks

        field_tuple = lambda t: '(' + ', '.join(['%s.`%s`' % (t, f) for f in fields[2:]]) + ')'

        query = 'UPDATE `blocks` AS b1 INNER JOIN `blocks_tmp` AS b2 ON b1.`name` = b2.`name` SET'
        query += ' ' + ', '.join(['b1.`{f}` = b2.`{f}`'.format(f = f) for f in fields[2:]])
        query += ' WHERE ' + field_tuple('b1') + ' != ' + field_tuple('b2')

        self._mysql.query(query)

        # there can be rows in blocks_tmp that are not in blocks
        query = 'INSERT INTO `blocks` (%s)' % (','.join(['`%s`' % f for f in fields]))
        query += ' SELECT * FROM `blocks_tmp` WHERE `name` NOT IN (SELECT `name` FROM `blocks`)'
        # using subquery is faster than left join + where right is null

        self._mysql.query(query)

        self._mysql.query('DROP TABLE `blocks_tmp`')

    def _do_save_replicas(self, sites, groups, datasets): #override
        # make name -> id maps for use later
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(datasets)
        if len(self._sites_to_ids) == 0:
            self._set_site_ids(sites)
        if len(self._groups_to_ids) == 0:
            self._set_group_ids(groups)

        # insert/update dataset replicas
        logger.info('Inserting/updating dataset replicas.')

        self._mysql.query('CREATE TABLE `dataset_replicas_new` LIKE `dataset_replicas`')

        fields = ('dataset_id', 'site_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        all_replicas = []
        for dataset in datasets:
            all_replicas.extend(dataset.replicas)

        self._mysql.insert_many('dataset_replicas_new', fields, mapping, all_replicas, do_update = False)

        self._mysql.query('RENAME TABLE `dataset_replicas` TO `dataset_replicas_old`')
        self._mysql.query('RENAME TABLE `dataset_replicas_new` TO `dataset_replicas`')
        self._mysql.query('DROP TABLE `dataset_replicas_old`')

        # insert/update block replicas
        logger.info('Inserting/updating block replicas.')

        # assuming block name is unique
        block_name_to_id = {}
        for block_id, block_name in self._mysql.query('SELECT DISTINCT b.`id`, b.`name` FROM `blocks` AS b INNER JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = b.`dataset_id`'):
            block_name_to_id[Block.translate_name(block_name)] = block_id

        all_replicas = []
        replica_sizes = []
        for dataset in datasets:
            for replica in dataset.replicas:
                site_id = self._sites_to_ids[replica.site]
                for block_replica in replica.block_replicas:
                    block_id = block_name_to_id[block_replica.block.name]

                    all_replicas.append((block_id, site_id, self._groups_to_ids[block_replica.group], block_replica.is_complete, block_replica.is_custodial))
                    if not block_replica.is_complete:
                        replica_sizes.append((block_id, site_id, block_replica.size))

        self._mysql.query('CREATE TABLE `block_replicas_new` LIKE `block_replicas`')

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda t: t

        try:
            self._mysql.insert_many('block_replicas_new', fields, mapping, all_replicas, do_update = False)
        except:
            # Unknown error occurred Aug 12 - trying to debug
            with open('/var/log/dynamo/block_replicas_new.dump', 'w') as dump:
                for did, r in blockreps_to_write:
                    dump.write('%d %s\n' % (did, str(r)))

            raise

        self._mysql.query('RENAME TABLE `block_replicas` TO `block_replicas_old`')
        self._mysql.query('RENAME TABLE `block_replicas_new` TO `block_replicas`')
        self._mysql.query('DROP TABLE `block_replicas_old`')

        self._mysql.query('CREATE TABLE `block_replica_sizes_new` LIKE `block_replica_sizes`')

        fields = ('block_id', 'site_id', 'size')
        mapping = lambda t: t

        self._mysql.insert_many('block_replica_sizes_new', fields, mapping, replica_sizes, do_update = False)

        self._mysql.query('RENAME TABLE `block_replica_sizes` TO `block_replica_sizes_old`')
        self._mysql.query('RENAME TABLE `block_replica_sizes_new` TO `block_replica_sizes`')
        self._mysql.query('DROP TABLE `block_replica_sizes_old`')

    def _do_save_replica_accesses(self, all_replicas): #override
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(list(set([r.dataset for r in all_replicas])))
        if len(self._sites_to_ids) == 0:
            self._set_site_ids(list(set([r.site for r in all_replicas])))

        self._mysql.query('CREATE TABLE `dataset_accesses_new` LIKE `dataset_accesses`')

        fields = ('dataset_id', 'site_id', 'date', 'access_type', 'num_accesses', 'cputime')

        for acc, access_type in [(DatasetReplica.ACC_LOCAL, 'local'), (DatasetReplica.ACC_REMOTE, 'remote')]:
            mapping = lambda (dataset_id, site_id, date, access): (dataset_id, site_id, date.strftime('%Y-%m-%d'), access_type, access.num_accesses, access.cputime)

            # instead of inserting by datasets or by sites, collect all access information into a single list
            all_accesses = []
            for replica in all_replicas:
                dataset_id = self._datasets_to_ids[replica.dataset]
                site_id = self._sites_to_ids[replica.site]
                for date, access in replica.accesses[acc].items():
                    all_accesses.append((dataset_id, site_id, date, access))

            self._mysql.insert_many('dataset_accesses_new', fields, mapping, all_accesses, do_update = False)

        self._mysql.query('RENAME TABLE `dataset_accesses` TO `dataset_accesses_old`')
        self._mysql.query('RENAME TABLE `dataset_accesses_new` TO `dataset_accesses`')
        self._mysql.query('DROP TABLE `dataset_accesses_old`')

        self._mysql.query('UPDATE `system` SET `dataset_accesses_last_update` = NOW()')

    def _do_save_dataset_requests(self, datasets): #override
        if len(self._datasets_to_ids) == 0:
            raise RuntimeError('save_dataset_requests cannot be called before initializing the id maps')

        all_requests = []
        for dataset in datasets:
            for request in dataset.requests:
                all_requests.append((dataset, request))

        self._mysql.query('CREATE TABLE `dataset_requests_new` LIKE `dataset_requests`')

        fields = ('id', 'dataset_id', 'queue_time', 'completion_time', 'nodes_total', 'nodes_done', 'nodes_failed', 'nodes_queued')
        mapping = lambda (d, r): (
            r.job_id,
            self._datasets_to_ids[d],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.queue_time)),
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.completion_time)) if r.completion_time > 0 else '0000-00-00 00:00:00',
            r.nodes_total,
            r.nodes_done,
            r.nodes_failed,
            r.nodes_queued
        )

        self._mysql.insert_many('dataset_requests_new', fields, mapping, all_requests, do_update = False)

        self._mysql.query('RENAME TABLE `dataset_requests` TO `dataset_requests_old`')
        self._mysql.query('RENAME TABLE `dataset_requests_new` TO `dataset_requests`')
        self._mysql.query('DROP TABLE `dataset_requests_old`')

        self._mysql.query('UPDATE `system` SET `dataset_requests_last_update` = NOW()')

    def _do_add_dataset_replicas(self, replicas): #override
        # make name -> id maps for use later
        if len(self._datasets_to_ids) == 0 or len(self._sites_to_ids) == 0 or len(self._groups_to_ids) == 0:
            raise RuntimeError('add_dataset_replicas cannot be called before initializing the id maps')

        # insert/update dataset replicas
        logger.info('Inserting/updating %d dataset replicas.', len(replicas))

        fields = ('dataset_id', 'site_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        self._mysql.insert_many('dataset_replicas', fields, mapping, replicas)

        # insert/update block replicas for non-complete dataset replicas
        all_replicas = []
        replica_sizes = []

        for replica in replicas:
            dataset_id = self._datasets_to_ids[replica.dataset]
            site_id = self._sites_to_ids[replica.site]
            
            block_ids = {}
            for name_str, block_id in self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id):
                block_ids[Block.translate_name(name_str)] = block_id

            # add the block replicas on this site to block_replicas together with SQL ID
            for block_replica in replica.block_replicas:
                block_id = block_ids[block_replica.block.name]

                all_replicas.append((block_id, site_id, self._groups_to_ids[block_replica.group], block_replica.is_complete, block_replica.is_custodial))
                if not block_replica.is_complete:
                    replica_sizes.append((block_id, site_id, block_replica.size))

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda t: t

        self._mysql.insert_many('block_replicas', fields, mapping, all_replicas)

        fields = ('block_id', 'site_id', 'size')
        mapping = lambda t: t

        self._mysql.insert_many('block_replica_sizes', fields, mapping, replica_sizes)

    def _do_delete_dataset(self, dataset): #override
        """
        Delete everything related to this dataset
        """
        try:
            dataset_id = self._mysql.query('SELECT `id` FROM `datasets` WHERE `name` LIKE %s', dataset.name)[0]
        except IndexError:
            return

        self._mysql.query('DELETE FROM br USING `block_replicas` AS br INNER JOIN `blocks` AS b ON b.`id` = br.`block_id` WHERE b.`dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM brs USING `block_replica_sizes` AS brs INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id` WHERE b.`dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM `blocks` WHERE `dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM `dataset_replicas` WHERE `dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM `datasets` WHERE `id` = %s', dataset_id)

    def _do_delete_datasets(self, datasets): #override
        """
        Delete everything related to the datasets
        """
        dataset_ids = self._mysql.select_many('datasets', 'id', 'name', ['\'%s\'' % d.name for d in datasets])

        ids_str = ','.join(['%d' % i for i in dataset_ids])

        self._mysql.query('DELETE FROM br USING `block_replicas` AS br INNER JOIN `blocks` AS b ON b.`id` = br.`block_id` WHERE b.`dataset_id` IN (%s)' % ids_str)
        self._mysql.query('DELETE FROM brs USING `block_replica_sizes` AS brs INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id` WHERE b.`dataset_id` IN (%s)' % ids_str)
        self._mysql.query('DELETE FROM `blocks` WHERE `dataset_id` IN (%s)' % ids_str)
        self._mysql.query('DELETE FROM `dataset_replicas` WHERE `dataset_id` IN (%s)' % ids_str)
        self._mysql.query('DELETE FROM `datasets` WHERE `id` IN (%s)' % ids_str)

    def _do_delete_block(self, block): #override
        query = 'SELECT b.`id` FROM `blocks` AS b INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        query += ' WHERE b.`name` LIKE %s AND d.`name` LIKE %s'

        try:
            block_id = self._mysql.query(query, block.real_name(), block.dataset.name)[0]
        except IndexError:
            return

        self._mysql.query('DELETE FROM `block_replicas` WHERE `block_id` = %s', block_id)
        self._mysql.query('DELETE FROM `block_replica_sizes` WHERE `block_id` = %s', block_id)
        self._mysql.query('DELETE FROM `blocks` WHERE `id` = %s', block_id)

    def _do_delete_datasetreplicas(self, site, datasets, delete_blockreplicas): #override
        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = self._mysql.select_many('datasets', 'id', 'name', ['\'%s\'' % d.name for d in datasets])

        self._mysql.delete_in('dataset_replicas', 'dataset_id', dataset_ids, additional_conditions = ['site_id = %d' % site_id])

        if delete_blockreplicas:
            ids_str = ','.join(['%d' % i for i in dataset_ids])
            self._mysql.query('DELETE FROM br USING `block_replicas` AS br INNER JOIN `blocks` AS b ON b.`id` = br.`block_id` WHERE b.`dataset_id` IN (%s) AND br.`site_id` = %d' % (ids_str, site_id))
            self._mysql.query('DELETE FROM brs USING `block_replica_sizes` AS brs INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id` WHERE b.`dataset_id` IN (%s) AND brs.`site_id` = %d' % (ids_str, site_id))

    def _do_delete_blockreplicas(self, replica_list): #override
        # Mass block replica deletion typically happens for a few sites and a few datasets.
        # Fetch site id first to avoid a long query.

        sites = list(set([r.site for r in replica_list])) # list of unique sites
        datasets = list(set([r.block.dataset for r in replica_list])) # list of unique sites

        site_names = ','.join(['\'%s\'' % s.name for s in sites])
        dataset_names = ','.join(['\'%s\'' % d.name for d in datasets])

        site_ids = {}
        dataset_ids = {}

        sql = 'SELECT `name`, `id` FROM `sites` WHERE `name` IN ({names})'
        result = self._mysql.query(sql.format(names = site_names))
        for site_name, site_id in result:
            site = next(s for s in sites if s.name == site_name)
            site_ids[site] = site_id

        sql = 'SELECT `name`, `id` FROM `datasets` WHERE `name` IN ({names})'
        result = self._mysql.query(sql.format(names = dataset_names))
        for dataset_name, dataset_id in result:
            dataset = next(d for d in datasets if d.name == dataset_name)
            dataset_ids[dataset] = dataset_id

        for site, site_id in site_ids.items():
            replicas_on_site = [r for r in replica_list if r.site == site]
            ids_str = ','.join(['%d' % dataset_ids[r.block.dataset] for r in replicas_on_site])

            sql = 'DELETE FROM br USING `block_replicas` AS br'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`'
            sql += ' WHERE br.`site_id` = %d AND b.`dataset_id` IN (%s)' % (site_id, ids_str)

            self._mysql.query(sql)

            sql = 'DELETE FROM brs USING `block_replica_sizes` AS brs'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id`'
            sql += ' WHERE brs.`site_id` = %d AND b.`dataset_id` IN (%s)' % (site_id, ids_str)

            self._mysql.query(sql)

    def _do_set_dataset_status(self, dataset_name, status_str): #override
        self._mysql.query('UPDATE `datasets` SET `status` = %s WHERE `name` LIKE %s', status_str, dataset_name)

    def _set_dataset_ids(self, datasets, update = False):
        # reset id maps to the current content in the DB.

        logger.debug('set_dataset_ids')

        if not update:
            self._datasets_to_ids = {}
            self._ids_to_datasets = {}

        name_to_id = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))

        for dataset in datasets:
            try:
                dataset_id = name_to_id[dataset.name]
            except KeyError:
                continue

            self._datasets_to_ids[dataset] = dataset_id
            self._ids_to_datasets[dataset_id] = dataset

    def _set_site_ids(self, sites):
        # reset id maps to the current content in the DB.

        self._sites_to_ids = {}
        self._ids_to_sites = {}

        name_to_id = dict(self._mysql.query('SELECT `name`, `id` FROM `sites`'))

        for site in sites:
            try:
                site_id = name_to_id[site.name]
            except KeyError:
                continue

            self._sites_to_ids[site] = site_id
            self._ids_to_sites[site_id] = site

     def _set_group_ids(self, groups):
        # reset id maps to the current content in the DB.

        self._groups_to_ids = {}
        self._ids_to_groups = {}

        ids_source = self._mysql.query('SELECT `name`, `id` FROM `groups`')
        for name, group_id in ids_source:
            try:
                group = next(g for g in groups if g.name == name)
            except StopIteration:
                continue

            self._groups_to_ids[group] = group_id
            self._ids_to_groups[group_id] = group

        self._groups_to_ids[None] = 0
        self._ids_to_groups[0] = None
