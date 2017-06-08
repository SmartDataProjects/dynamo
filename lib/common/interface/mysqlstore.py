import os
import time
import datetime
import re
import socket
import logging
import fnmatch
import pprint

from common.interface.store import LocalStoreInterface
from common.interface.mysql import MySQL
from common.dataformat import Dataset, Block, File, Site, Group, DatasetReplica, BlockReplica
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

    def _do_get_last_update(self): #override
        return self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

    def _do_set_last_update(self, tm): #override
        self._mysql.query('UPDATE `system` SET `last_update` = FROM_UNIXTIME(%d)' % int(tm))

    def _do_get_site_list(self, include, exclude): #override
        # Load sites
        site_names = []

        names = self._mysql.query('SELECT `name` FROM `sites`')

        for name in names:
            if name in exclude:
                continue

            for filt in include:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                continue

            site_names.append(name)
        
        return site_names

    def _do_load_data(self, site_filt, dataset_filt, load_blocks, load_files, load_replicas): #override
        # First set last_update
        self.last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

        if type(site_filt) is list and len(site_filt) == 0:
            return [], [], []

        # Load sites
        site_list = []

        query = 'SELECT `name`, `host`, `storage_type`+0, `backend`, `storage`, `cpu`, `status`+0 FROM `sites`'
        if type(site_filt) is str and site_filt != '*' and site_filt != '':
            query += ' WHERE `name` LIKE \'%s\'' % site_filt.replace('*', '%%')
        elif type(site_filt) is list:
            query += ' WHERE `name` IN (%s)' % (','.join('\'%s\'' % s for s in site_filt))

        for name, host, storage_type, backend, storage, cpu, status in self._mysql.query(query):
            site = Site(name, host = host, storage_type = Site.storage_type_val(storage_type), backend = backend, storage = storage, cpu = cpu, status = status)
            site_list.append(site)

        logger.info('Loaded data for %d sites.', len(site_list))

        if len(site_list) == 0:
            return [], [], []

        id_site_map = {}
        self._make_site_map(site_list, id_site_map = id_site_map)

        sites_str = ','.join(['%d' % i for i in id_site_map])

        # Load groups
        group_list = []

        for name, olname in self._mysql.query('SELECT `name`, `olevel` FROM `groups`'):
            if olname == 'Dataset':
                olevel = Dataset
            else:
                olevel = Block

            group = Group(name, olevel)
            group_list.append(group)

        logger.info('Loaded data for %d groups.', len(group_list))

        id_group_map = {}
        self._make_group_map(group_list, id_group_map = id_group_map)

        # Load site quotas
        quotas = self._mysql.query('SELECT q.`site_id`, p.`name`, q.`storage` FROM `quotas` AS q INNER JOIN `partitions` AS p ON p.`id` = q.`partition_id`')
        for site_id, partition_name, storage in quotas:
            try:
                site = id_site_map[site_id]
            except KeyError:
                continue

            try:
                partition = Site.partitions[partition_name]
            except KeyError:
                continue

            site.set_partition_quota(partition, storage)

        # Load software versions - treat directly as tuples with id in first column
        software_version_map = {0: None}
        for vtuple in self._mysql.query('SELECT * FROM `software_versions`'):
            software_version_map[vtuple[0]] = vtuple[1:]

        # Load datasets - only load ones with replicas on selected sites if load_replicas == True
        dataset_list = []

        query = 'SELECT DISTINCT d.`name`, d.`size`, d.`num_files`, d.`status`+0, d.`on_tape`, d.`data_type`+0, d.`software_version_id`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open`'
        query += ' FROM `datasets` AS d'
        conditions = []
        if load_replicas:
            query += ' INNER JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = d.`id`'
        if dataset_filt != '/*/*/*' and dataset_filt != '':
            conditions.append('d.`name` LIKE \'%s\'' % dataset_filt.replace('*', '%%'))

        if len(conditions) != 0:
            query += ' WHERE ' + (' AND '.join(conditions))

        for name, size, num_files, status, on_tape, data_type, software_version_id, last_update, is_open in self._mysql.query(query):
            dataset = Dataset(name, size = size, num_files = num_files, status = int(status), on_tape = on_tape, data_type = int(data_type), last_update = last_update, is_open = (is_open == 1))
            dataset.software_version = software_version_map[software_version_id]

            dataset_list.append(dataset)

        logger.info('Loaded data for %d datasets.', len(dataset_list))

        if len(dataset_list) == 0:
            return site_list, group_list, dataset_list

        id_dataset_map = {}

        if load_blocks or load_files or load_replicas:
            # Load blocks
            logger.info('Loading blocks.')
    
            self._make_dataset_map(dataset_list, id_dataset_map = id_dataset_map)
    
            block_id_maps = {} # {dataset_id: {block_id: block}}
    
            query = 'SELECT DISTINCT b.`id`, b.`dataset_id`, b.`name`, b.`size`, b.`num_files`, b.`is_open` FROM `blocks` AS b'
            conditions = []
            if load_replicas:
                query += ' INNER JOIN `block_replicas` AS br ON br.`block_id` = b.`id`'
                conditions.append('br.`site_id` IN (%s)' % sites_str)
            if dataset_filt != '/*/*/*' and dataset_filt != '':
                query += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
                conditions.append('d.`name` LIKE \'%s\'' % dataset_filt.replace('*', '%%'))
    
            if len(conditions) != 0:
                query += ' WHERE ' + (' AND '.join(conditions))
    
            query += ' ORDER BY b.`dataset_id`'
    
            num_blocks = 0
    
            start = time.time()
            results = self._mysql.query(query)
            logger.info('Query took %.1f seconds.', time.time() - start)
    
            _dataset_id = 0
            dataset = None
            for block_id, dataset_id, name, size, num_files, is_open in results:
                if dataset_id != _dataset_id:
                    try:
                        dataset = id_dataset_map[dataset_id]
                    except KeyError: # inconsistent record (orphan block)
                        continue
                       
                    block_id_map = {}
                    block_id_maps[dataset_id] = block_id_map
                    _dataset_id = dataset_id

                    dataset.blocks = []
                    dataset.size = 0
                    dataset.num_files = 0
    
                block = Block(Block.translate_name(name), dataset, size, num_files, is_open)
    
                dataset.blocks.append(block)
                dataset.size += block.size
                dataset.num_files += block.num_files

                block_id_map[block_id] = block
    
                num_blocks += 1
    
            logger.info('Loaded data for %d blocks in %.1f seconds.', num_blocks, time.time() - start)

        if load_files:
            logger.info('Loading files.')
            start = time.time()

            num_files = 0
            for dataset in dataset_list:
                self._do_load_files(dataset)
                num_files += len(dataset.files)

            logger.info('Loaded data for %d files in %.1f seconds.', num_files, time.time() - start)

        if load_replicas:
            logger.info('Loading replicas.')

            if len(id_dataset_map) == 0:
                self._make_dataset_map(dataset_list, id_dataset_map = id_dataset_map)

            sql = 'SELECT dr.`dataset_id`, dr.`site_id`, dr.`completion`, dr.`is_custodial`, UNIX_TIMESTAMP(dr.`last_block_created`),'
            sql += ' br.`block_id`, br.`group_id`, br.`is_complete`, br.`is_custodial`, brs.`size`'
            sql += ' FROM `dataset_replicas` AS dr'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`'
            sql += ' INNER JOIN `blocks` AS b ON b.`dataset_id` = d.`id`'
            sql += ' INNER JOIN `block_replicas` AS br ON (br.`block_id`, br.`site_id`) = (b.`id`, dr.`site_id`)'
            sql += ' LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (br.`block_id`, br.`site_id`)'

            conditions = ['dr.`site_id` IN (%s)' % sites_str]
            if dataset_filt != '/*/*/*' and dataset_filt != '':
                conditions.append('dr.`dataset_id` IN (%s)' % (','.join(['%d' % i for i in id_dataset_map.keys()])))

            if len(conditions) != 0:
                sql += ' WHERE ' + (' AND '.join(conditions))

            sql += ' ORDER BY dr.`dataset_id`, dr.`site_id`'

            _dataset_id = 0
            _site_id = 0
            dataset_replica = None
    
            for dataset_id, site_id, completion, is_custodial, last_block_created, block_id, group_id, is_complete, b_is_custodial, b_size in self._mysql.query(sql):
                if dataset_id != _dataset_id:
                    _dataset_id = dataset_id
                    dataset = id_dataset_map[_dataset_id]
                    dataset.replicas = []

                    block_id_map = block_id_maps[dataset_id]

                if site_id != _site_id:
                    _site_id = site_id
                    site = id_site_map[site_id]

                if dataset_replica is None or dataset != dataset_replica.dataset or site != dataset_replica.site:
                    dataset_replica = DatasetReplica(dataset, site, is_complete = (completion != 'incomplete'), is_custodial = is_custodial, last_block_created = last_block_created)

                    dataset.replicas.append(dataset_replica)
                    site.dataset_replicas.add(dataset_replica)

                block = block_id_map[block_id]

                group = id_group_map[group_id]

                block_replica = BlockReplica(block, site, group = group, is_complete = is_complete, is_custodial = b_is_custodial, size = block.size if b_size is None else b_size)

                dataset_replica.block_replicas.append(block_replica)
                site.add_block_replica(block_replica)

        # Only the list of sites, groups, and datasets are returned
        return site_list, group_list, dataset_list

    def _do_load_dataset(self, dataset_name, load_blocks, load_files):
        query = 'SELECT d.`size`, d.`num_files`, d.`status`+0, d.`on_tape`, d.`data_type`+0, s.`cycle`, s.`major`, s.`minor`, s.`suffix`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open` FROM `datasets` AS d'
        query += ' LEFT JOIN `software_versions` AS s ON s.`id` = d.`software_version_id`'
        query += ' WHERE d.`name` = %s'
        result = self._mysql.query(query, dataset_name)

        if len(result) == 0:
            return None

        size, num_files, status, on_tape, data_type, s_cycle, s_major, s_minor, s_suffix, last_update, is_open = result[0]
        dataset = Dataset(dataset_name, size = size, num_files = num_files, status = int(status), on_tape = on_tape, data_type = int(data_type), last_update = last_update, is_open = (is_open == 1))
        if s_cycle is None:
            dataset.software_version = None
        else:
            dataset.software_version = (s_cycle, s_major, s_minor, s_suffix)

        if load_blocks:
            self._do_load_blocks(dataset)

        if load_files:
            self._do_load_files(dataset)

        return dataset

    def _do_load_blocks(self, dataset):
        if dataset.blocks is not None:
            # clear out the existing blocks
            for block in list(dataset.blocks):
                dataset.remove_block(block)

        dataset.blocks = None
        dataset.size = 0
        dataset.num_files = 0

        result = self._mysql.query('SELECT `id` FROM `datasets` WHERE `name` = %s', dataset.name)
        if len(result) == 0:
            return

        dataset_id = result[0]

        result = self._mysql.query('SELECT `name`, `size`, `num_files`, `is_open` FROM `blocks` WHERE `dataset_id` = %d' % dataset_id)

        dataset.blocks = []

        for name, size, num_files, is_open in result:
            dataset.blocks.append(Block(Block.translate_name(name), dataset, size, num_files, is_open == 1))
            dataset.size += size
            dataset.num_files += num_files

    def _do_load_files(self, dataset): #override
        dataset.files = set()

        query = 'SELECT `id` FROM `datasets` WHERE `name` = %s'
        results = self._mysql.query(query, dataset.name)

        if len(results) == 0:
            return

        dataset_id = results[0]

        block_map = dict((b.real_name(), b) for b in dataset.blocks)

        block_id_map = dict()
        for block_id, name in self._mysql.query('SELECT `id`, `name` FROM `blocks` WHERE `dataset_id` = %d' % dataset_id):
            try:
                block_id_map[block_id] = block_map[name]
            except KeyError:
                continue

        # Load files
        query = 'SELECT `block_id`, `name`, `size` FROM `files` WHERE `dataset_id` = %d ORDER BY `block_id`' % dataset_id

        _block_id = 0
        block = None
        for block_id, name, size in self._mysql.query(query):
            if block_id != _block_id:
                try:
                    block = block_id_map[block_id]
                except KeyError:
                    continue

                _block_id = block_id

            lfile = File.create(name, block, size)

            dataset.files.add(lfile)

    def _do_find_block_of(self, fullpath, datasets): #override
        query = 'SELECT d.`name`, b.`name` FROM `files` AS f'
        query += ' INNER JOIN `datasets` AS d ON d.`id` = f.`dataset_id`'
        query += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        query += ' WHERE f.`name` = %s'

        result = self._mysql.query(query, fullpath)

        if len(result) == 0:
            return None

        dname, bname = result[0]

        try:
            dataset = datasets[dname]
        except KeyError:
            return None

        if dataset.blocks is None:
            self.load_blocks(dataset)

        return dataset.find_block(Block.translate_name(bname))

    def _do_load_replica_accesses(self, sites, datasets): #override
        id_site_map = {}
        self._make_site_map(sites, id_site_map = id_site_map)
        id_dataset_map = {}
        self._make_dataset_map(datasets, id_dataset_map = id_dataset_map)

        for dataset in datasets:
            if dataset.replicas is None:
                continue

        access_list = {}

        # pick up all accesses that are less than 1 year old
        # old accesses will eb removed automatically next time the access information is saved from memory
        records = self._mysql.query('SELECT `dataset_id`, `site_id`, YEAR(`date`), MONTH(`date`), DAY(`date`), `access_type`+0, `num_accesses` FROM `dataset_accesses` WHERE `date` > DATE_SUB(NOW(), INTERVAL 2 YEAR) ORDER BY `dataset_id`, `site_id`, `date`')

        # little speedup by not repeating lookups for the same replica
        current_dataset_id = 0
        current_site_id = 0
        replica = None
        for dataset_id, site_id, year, month, day, access_type, num_accesses in records:
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

        logger.info('Loaded %d replica access data. Last update on %s UTC', len(records), time.strftime('%Y-%m-%d', time.gmtime(last_update)))

        return (last_update, access_list)

    def _do_load_dataset_requests(self, datasets): #override
        id_dataset_map = {}
        self._make_dataset_map(datasets, id_dataset_map = id_dataset_map)

        # pick up requests that are less than 1 year old
        # old requests will be removed automatically next time the access information is saved from memory
        records = self._mysql.query('SELECT `dataset_id`, `id`, UNIX_TIMESTAMP(`queue_time`), UNIX_TIMESTAMP(`completion_time`), `nodes_total`, `nodes_done`, `nodes_failed`, `nodes_queued` FROM `dataset_requests` WHERE `queue_time` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `queue_time`')

        requests = {}

        # little speedup by not repeating lookups for the same dataset
        current_dataset_id = 0
        for dataset_id, job_id, queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued in records:
            if dataset_id != current_dataset_id:
                try:
                    dataset = id_dataset_map[dataset_id]
                except KeyError:
                    continue

                current_dataset_id = dataset_id
                requests[dataset] = {}

            requests[dataset][job_id] = (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued)

        last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`dataset_requests_last_update`) FROM `system`')[0]

        logger.info('Loaded %d dataset request data. Last update at %s UTC', len(records), time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(last_update)))

        return (last_update, requests)

    def _do_save_sites(self, sites): #override
        # insert/update sites
        logger.info('Inserting/updating %d sites.', len(sites))

        fields = ('name', 'host', 'storage_type', 'backend', 'storage', 'cpu', 'status')
        mapping = lambda s: (s.name, s.host, Site.storage_type_name(s.storage_type), s.backend, s.storage, s.cpu, s.status)

        self._mysql.insert_many('sites', fields, mapping, sites)

    def _do_save_groups(self, groups): #override
        # insert/update groups
        logger.info('Inserting/updating %d groups.', len(groups))

        self._mysql.insert_many('groups', ('name', 'olevel'), lambda g: (g.name, g.olevel.__name__), groups)

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

        # first delete datasets in UNKNOWN status if there are any
        self._mysql.query('DELETE FROM `datasets` WHERE `status` = \'UNKNOWN\'')

        name_entry_map = {}
        query = 'SELECT `name`, `id`, `size`, `num_files`, `status`+0, `on_tape`, `data_type`+0, `software_version_id`, UNIX_TIMESTAMP(`last_update`), `is_open` FROM `datasets`'
        for entry in self._mysql.query(query):
            name_entry_map[entry[0]] = entry[1:]

        dataset_ids_to_delete = []
        datasets_to_update = []
        datasets_to_insert = []

        for dataset in datasets:
            if '/MuOnia/Run2016E-18Apr2017-v1' in dataset.name:
                logger.info("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE WHAT THE FUCK IS GOING ON EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
            try:
                dataset_id, size, num_files, status, on_tape, data_type, software_version_id, last_update, is_open = name_entry_map[dataset.name]
            except KeyError:
                if dataset.status != Dataset.STAT_UNKNOWN:
                    datasets_to_insert.append(dataset)
                continue

            if dataset.status == Dataset.STAT_UNKNOWN:
                dataset_ids_to_delete.append(dataset_id)
                continue

            if dataset.size != size or dataset.num_files != num_files or dataset.status != status or dataset.on_tape != on_tape or \
                    version_map[dataset.software_version] != software_version_id or dataset.last_update != last_update or dataset.is_open != is_open:
                datasets_to_update.append((dataset_id, dataset))

        if len(dataset_ids_to_delete) != 0:
            self._mysql.delete_many('datasets', 'id', dataset_ids_to_delete)

        logger.info("DATASETS TO UPDATE:")    
        logger.info(pprint.pformat(datasets_to_update))
        logger.info("DATASETS TO INSERT:")    
        logger.info(pprint.pformat(datasets_to_insert))

        # clean up orphans before making insertions
        self._mysql.query('DELETE FROM `blocks` WHERE `dataset_id` NOT IN (SELECT `id` FROM `datasets`)')
        self._mysql.query('DELETE FROM `files` WHERE `dataset_id` NOT IN (SELECT `id` FROM `datasets`)')
        self._mysql.query('DELETE FROM `files` WHERE `block_id` NOT IN (SELECT `id` FROM `blocks`)')

        fields = ('id', 'name', 'size', 'num_files', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda (i, d): (
            i,
            d.name,
            d.size,
            d.num_files,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
            1 if d.is_open else 0
        )

        # use INSERT ON DUPLICATE KEY UPDATE
        self._mysql.insert_many('datasets', fields, mapping, datasets_to_update, do_update = True)

        fields = ('name', 'size', 'num_files', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda d: (
            d.name,
            d.size,
            d.num_files,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
            1 if d.is_open else 0
        )

        self._mysql.insert_many('datasets', fields, mapping, datasets_to_insert)

        # load the dataset ids
        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        # insert/update blocks and files
        logger.info('Inserting/updating blocks and files.')

        # speedup - fetch all blocks and files of updated datasets
        # if size, num_file, or is_open of a block or a file is updated, its dataset also is

        pool = [did for did, dataset in datasets_to_update if dataset.blocks is not None]
        block_entries = dict((i, []) for i in pool)

        _dataset_id = 0
        for entry in self._mysql.select_many('blocks', ('dataset_id', 'id', 'name', 'size', 'num_files', 'is_open'), 'dataset_id', pool, order_by = '`dataset_id`'):
            if entry[0] != _dataset_id:
                _dataset_id = entry[0]
                entry_list = block_entries[entry[0]] = []

            entry_list.append(entry[1:])

        pool = [did for did, dataset in datasets_to_update if dataset.files is not None]
        file_entries = dict((i, []) for i in pool)

        _dataset_id = 0
        for entry in self._mysql.select_many('files', ('dataset_id', 'id', 'size', 'name'), 'dataset_id', pool, order_by = '`dataset_id`'):
            if entry[0] != _dataset_id:
                _dataset_id = entry[0]
                entry_list = file_entries[entry[0]] = []

            entry_list.append(entry[1:])

        block_ids_to_delete = []
        blocks_to_update = []
        file_ids_to_delete = []
        files_to_update = []
        files_to_insert = []

        for dataset_id, dataset in datasets_to_update:
            if dataset.blocks is None:
                continue

            blocks = dict((b.real_name(), b) for b in dataset.blocks)
            block_id_map = {}

            for block_id, name, size, num_files, is_open in block_entries[dataset_id]:
                try:
                    block = blocks.pop(name)
                except KeyError:
                    block_ids_to_delete.append(block_id)
                    continue

                block_id_map[block] = block_id
        
                if size != block.size or num_files != block.num_files or is_open != block.is_open:
                    blocks_to_update.append((block_id, name, block.size, block.num_files, block.is_open))

            for name, block in blocks.items():
                block_id_map[block] = self._mysql.query('INSERT INTO `blocks` (`dataset_id`, `name`, `size`, `num_files`, `is_open`) VALUES (%s, %s, %s, %s, %s)',
                    dataset_id, name, block.size, block.num_files, block.is_open)

            if dataset.files is None:
                continue
        
            files = dict((f.fullpath(), f) for f in dataset.files)
            
            for file_id, size, name in file_entries[dataset_id]:
                try:
                    lfile = files.pop(name)
                except KeyError:
                    file_ids_to_delete.append(file_id)
                    continue
                    
                if size != lfile.size:
                    files_to_update.append((file_id, lfile.size, name))

            for name, lfile in files.items():
                files_to_insert.append((block_id_map[lfile.block], dataset_id, lfile.size, name))

        for dataset in datasets_to_insert:
            if dataset.blocks is None:
                continue

            dataset_id = dataset_id_map[dataset]

            block_id_map = {}

            for block in dataset.blocks:
                block_id_map[block] = self._mysql.query('INSERT INTO `blocks` (`dataset_id`, `name`, `size`, `num_files`, `is_open`) VALUES (%s, %s, %s, %s, %s)',
                    dataset_id, block.real_name(), block.size, block.num_files, block.is_open)

            if dataset.files is None:
                continue

            for lfile in dataset.files:
                files_to_insert.append((block_id_map[lfile.block], dataset_id, lfile.size, lfile.fullpath()))

        self._mysql.delete_many('blocks', 'id', block_ids_to_delete)
        self._mysql.delete_many('files', 'id', file_ids_to_delete)
        
        # update blocks
        fields = ('id', 'name', 'size', 'num_files', 'is_open')
        # use INSERT ON DUPLICATE KEY UPDATE query
        self._mysql.insert_many('blocks', fields, None, blocks_to_update, do_update = True)

        # update files
        fields = ('id', 'size', 'name')
        self._mysql.insert_many('files', fields, None, files_to_update, do_update = True)

        # insert files
        fields = ('block_id', 'dataset_id', 'size', 'name')
        self._mysql.insert_many('files', fields, None, files_to_insert, do_update = False)


    def _do_update_replicas(self, sites, groups, datasets): #override
        site_id_map = {}
        self._make_site_map(sites, site_id_map = site_id_map)
        group_id_map = {}
        self._make_group_map(groups, group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        # insert/update dataset replicas
        logger.info('Updating dataset replicas.')

        if self._mysql.table_exists('dataset_replicas_new'):
            self._mysql.query('DROP TABLE `dataset_replicas_new`')

        fields = ('dataset_id', 'site_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (dataset_id_map[r.dataset], site_id_map[r.site], 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        all_replicas = []
        for dataset in datasets:
            if dataset.status != Dataset.STAT_UNKNOWN and dataset.replicas is not None:
                all_replicas.extend(dataset.replicas)

        self._mysql.insert_many('dataset_replicas', fields, mapping, all_replicas, do_update = True)

        # insert/update block replicas
        logger.info('Updating block replicas.')

        # assuming block name is unique
        block_name_to_id = {}
        for block_id, block_name in self._mysql.query('SELECT DISTINCT b.`id`, b.`name` FROM `blocks` AS b INNER JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = b.`dataset_id`'):
            block_name_to_id[Block.translate_name(block_name)] = block_id

        all_replicas = []
        replica_sizes = []
        for dataset in datasets:
            if dataset.status == Dataset.STAT_UNKNOWN or dataset.replicas is None:
                continue

            for replica in dataset.replicas:
                site_id = site_id_map[replica.site]
                for block_replica in replica.block_replicas:
                    block_id = block_name_to_id[block_replica.block.name]

                    all_replicas.append((block_id, site_id, group_id_map[block_replica.group], block_replica.is_complete, block_replica.is_custodial))
                    if not block_replica.is_complete:
                        replica_sizes.append((block_id, site_id, block_replica.size))

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        self._mysql.insert_many('block_replicas', fields, None, all_replicas, do_update = True)

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes', fields, None, replica_sizes, do_update = True)


    def _do_save_replicas(self, sites, groups, datasets): #override
        site_id_map = {}
        self._make_site_map(sites, site_id_map = site_id_map)
        group_id_map = {}
        self._make_group_map(groups, group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        # insert/update dataset replicas
        logger.info('Inserting/updating dataset replicas.')

        if self._mysql.table_exists('dataset_replicas_new'):
            self._mysql.query('DROP TABLE `dataset_replicas_new`')

        self._mysql.query('CREATE TABLE `dataset_replicas_new` LIKE `dataset_replicas`')

        fields = ('dataset_id', 'site_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (dataset_id_map[r.dataset], site_id_map[r.site], 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        all_replicas = []
        for dataset in datasets:
            if dataset.status != Dataset.STAT_UNKNOWN and dataset.replicas is not None:
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
            if dataset.status == Dataset.STAT_UNKNOWN or dataset.replicas is None:
                continue

            for replica in dataset.replicas:
                site_id = site_id_map[replica.site]
                for block_replica in replica.block_replicas:
                    block_id = block_name_to_id[block_replica.block.name]

                    all_replicas.append((block_id, site_id, group_id_map[block_replica.group], block_replica.is_complete, block_replica.is_custodial))
                    if not block_replica.is_complete:
                        replica_sizes.append((block_id, site_id, block_replica.size))

        if self._mysql.table_exists('block_replicas_new'):
            self._mysql.query('DROP TABLE `block_replicas_new`')

        self._mysql.query('CREATE TABLE `block_replicas_new` LIKE `block_replicas`')

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        self._mysql.insert_many('block_replicas_new', fields, None, all_replicas, do_update = False)

        self._mysql.query('RENAME TABLE `block_replicas` TO `block_replicas_old`')
        self._mysql.query('RENAME TABLE `block_replicas_new` TO `block_replicas`')
        self._mysql.query('DROP TABLE `block_replicas_old`')

        if self._mysql.table_exists('block_replica_sizes_new'):
            self._mysql.query('DROP TABLE `block_replica_sizes_new`')

        self._mysql.query('CREATE TABLE `block_replica_sizes_new` LIKE `block_replica_sizes`')

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes_new', fields, None, replica_sizes, do_update = False)

        self._mysql.query('RENAME TABLE `block_replica_sizes` TO `block_replica_sizes_old`')
        self._mysql.query('RENAME TABLE `block_replica_sizes_new` TO `block_replica_sizes`')
        self._mysql.query('DROP TABLE `block_replica_sizes_old`')

    def _do_save_replica_accesses(self, access_list): #override
        replicas = access_list.keys()

        site_id_map = {}
        self._make_site_map(list(set(r.site for r in replicas)), site_id_map = site_id_map)
        dataset_id_map = {}
        self._make_dataset_map(list(set(r.dataset for r in replicas)), dataset_id_map = dataset_id_map)

        fields = ('dataset_id', 'site_id', 'date', 'access_type', 'num_accesses', 'cputime')

        data = []
        for replica, replica_access_list in access_list.items():
            dataset_id = dataset_id_map[replica.dataset]
            site_id = site_id_map[replica.site]

            for date, (num_accesses, cputime) in replica_access_list.items():
                data.append((dataset_id, site_id, date.strftime('%Y-%m-%d'), 'local', num_accesses, cputime))

        self._mysql.insert_many('dataset_accesses', fields, None, data, do_update = True)

        # remove old entries
        self._mysql.query('DELETE FROM `dataset_accesses` WHERE `date` < DATE_SUB(NOW(), INTERVAL 2 YEAR)')
        self._mysql.query('UPDATE `system` SET `dataset_accesses_last_update` = NOW()')

    def _do_save_dataset_requests(self, request_list): #override
        datasets = request_list.keys()

        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        fields = ('id', 'dataset_id', 'queue_time', 'completion_time', 'nodes_total', 'nodes_done', 'nodes_failed', 'nodes_queued')

        data = []
        for dataset, dataset_request_list in request_list.items():
            dataset_id = dataset_id_map[dataset]

            for job_id, (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued) in dataset_request_list.items():
                data.append((
                    job_id,
                    dataset_id,
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(queue_time)),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(completion_time)) if completion_time > 0 else '0000-00-00 00:00:00',
                    nodes_total,
                    nodes_done,
                    nodes_failed,
                    nodes_queued
                ))

        self._mysql.insert_many('dataset_requests', fields, None, data, do_update = True)

        self._mysql.query('DELETE FROM `dataset_requests` WHERE `queue_time` < DATE_SUB(NOW(), INTERVAL 1 YEAR)')
        self._mysql.query('UPDATE `system` SET `dataset_requests_last_update` = NOW()')

    def _do_add_datasetreplicas(self, replicas): #override
        site_id_map = {}
        self._make_site_map(list(set(r.site for r in replicas)), site_id_map = site_id_map)
        groups = set()
        for replica in replicas:
            groups.update(block_replica.group for block_replica in replica.block_replicas)
        group_id_map = {}
        self._make_group_map(list(groups), group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(list(set(r.dataset for r in replicas)), dataset_id_map = dataset_id_map)

        # insert/update dataset replicas
        logger.info('Inserting/updating %d dataset replicas.', len(replicas))

        fields = ('dataset_id', 'site_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (dataset_id_map[r.dataset], site_id_map[r.site], 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        self._mysql.insert_many('dataset_replicas', fields, mapping, replicas)

        # insert/update block replicas
        all_replicas = []
        replica_sizes = []

        for replica in replicas:
            dataset_id = dataset_id_map[replica.dataset]
            site_id = site_id_map[replica.site]
            
            block_ids = {}
            for name_str, block_id in self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id):
                block_ids[Block.translate_name(name_str)] = block_id

            # add the block replicas on this site to block_replicas together with SQL ID
            for block_replica in replica.block_replicas:
                block_id = block_ids[block_replica.block.name]

                all_replicas.append((block_id, site_id, group_id_map[block_replica.group], block_replica.is_complete, block_replica.is_custodial))
                if not block_replica.is_complete:
                    replica_sizes.append((block_id, site_id, block_replica.size))

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        self._mysql.insert_many('block_replicas', fields, None, all_replicas)

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes', fields, None, replica_sizes)

    def _do_add_blockreplicas(self, replicas): #override
        site_id_map = {}
        self._make_site_map(list(set(r.site for r in replicas)), site_id_map = site_id_map)
        group_id_map = {}
        self._make_group_map(list(set(r.group for r in replicas)), group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(list(set(r.block.dataset for r in replicas)), dataset_id_map = dataset_id_map)

        all_replicas = []
        replica_sizes = []

        for replica in replicas:
            dataset_id = dataset_id_map[replica.block.dataset]
            site_id = site_id_map[replica.site]
            
            block_ids = {}
            for name_str, block_id in self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id):
                block_ids[Block.translate_name(name_str)] = block_id

            block_id = block_ids[replica.block.name]

            all_replicas.append((block_id, site_id, group_id_map[replica.group], replica.is_complete, replica.is_custodial))
            if not replica.is_complete:
                replica_sizes.append((block_id, site_id, replica.size))

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        self._mysql.insert_many('block_replicas', fields, None, all_replicas)

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes', fields, None, replica_sizes)

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
        dataset_ids = self._mysql.select_many('datasets', 'id', 'name', [d.name for d in datasets])

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

        dataset_ids = self._mysql.select_many('datasets', 'id', 'name', [d.name for d in datasets])

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

    def _make_site_map(self, sites, site_id_map = None, id_site_map = None):
        self._make_map('sites', sites, site_id_map, id_site_map)

    def _make_group_map(self, groups, group_id_map = None, id_group_map = None):
        self._make_map('groups', groups, group_id_map, id_group_map)
        if group_id_map is not None:
            group_id_map[None] = 0
        if id_group_map is not None:
            id_group_map[0] = None

    def _make_dataset_map(self, datasets, dataset_id_map = None, id_dataset_map = None):
        self._make_map('datasets', datasets, dataset_id_map, id_dataset_map, tmp_join = (len(datasets) < 1000))

    def _make_map(self, table, objects, object_id_map, id_object_map, tmp_join = False):
        logger.debug('make_map %s (%d) obejcts', table, len(objects))

        if len(objects) == 0:
            return

        if tmp_join:
            tmp_table = '%s_map_tmp' % table
            if self._mysql.table_exists(tmp_table):
                self._mysql.query('DROP TABLE `%s`' % tmp_table)

            self._mysql.query('CREATE TABLE `%s` (`name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL, PRIMARY KEY (`name`)) ENGINE=MyISAM DEFAULT CHARSET=latin1' % tmp_table)
            obj_names = [(obj.name,) for obj in objects]
            self._mysql.insert_many(tmp_table, ('name',), None, obj_names)

            name_to_id = dict(self._mysql.query('SELECT t1.`name`, t1.`id` FROM `%s` AS t1 INNER JOIN `%s` AS t2 ON t2.`name` = t1.`name`' % (table, tmp_table)))

            self._mysql.query('DROP TABLE `%s`' % tmp_table)

        else:
            name_to_id = dict(self._mysql.query('SELECT `name`, `id` FROM `%s`' % table))

        for obj in objects:
            try:
                obj_id = name_to_id[obj.name]
            except KeyError:
                continue

            if object_id_map is not None:
                object_id_map[obj] = obj_id
            if id_object_map is not None:
                id_object_map[obj_id] = obj
