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

        self._db_name = config.mysqlstore.db_params['db']

        self.last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0] # MySQL displays last_update in local time, but returns the UTC timestamp

        self._datasets_to_ids = {} # cache dictionary object -> mysql id
        self._sites_to_ids = {} # cache dictionary object -> mysql id
        self._groups_to_ids = {} # cache dictionary object -> mysql id
        self._ids_to_datasets = {} # cache dictionary mysql id -> object
        self._ids_to_sites = {} # cache dictionary mysql id -> object
        self._ids_to_groups = {} # cache dictionary mysql id -> object

    def _do_acquire_lock(self): #override
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

            logger.warning('Failed to lock database. Waiting 30 seconds..')

            time.sleep(30)

    def _do_release_lock(self): #override
        self._mysql.query('LOCK TABLES `system` WRITE')
        self._mysql.query('UPDATE `system` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `system`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise LocalStoreInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_make_snapshot(self, timestamp, clear): #override
        snapshot_db = self._db_name + '_' + timestamp

        self._mysql.query('CREATE DATABASE `{copy}`'.format(copy = snapshot_db))

        tables = self._mysql.query('SHOW TABLES')

        for table in tables:
            print '  ' + table

            self._mysql.query('CREATE TABLE `{copy}`.`{table}` LIKE `{orig}`.`{table}`'.format(copy = snapshot_db, orig = self._db_name, table = table))

            if table == 'system':
                self._mysql.query('INSERT INTO `{copy}`.`system` (`last_update`) SELECT `last_update` FROM `{orig}`.`system`'.format(copy = snapshot_db, orig = self._db_name))
                continue

            else:
                self._mysql.query('INSERT INTO `{copy}`.`{table}` SELECT * FROM `{orig}`.`{table}`'.format(copy = snapshot_db, orig = self._db_name, table = table))

            if clear == LocalStoreInterface.CLEAR_ALL or \
               (clear == LocalStoreInterface.CLEAR_REPLICAS and table in ['dataset_replicas', 'block_replicas']):
                # drop the original table and copy back the format from the snapshot
                print '   TRUNCATE'
                self._mysql.query('TRUNCATE TABLE `{orig}`.`{table}`'.format(orig = self._db_name, table = table))

        print ' Done.'

    def _do_remove_snapshot(self, newer_than, older_than): #override
        snapshots = self._do_list_snapshots()

        for snapshot in snapshots:
            tm = int(time.mktime(time.strptime(snapshot, '%y%m%d%H%M%S')))
            if (newer_than == older_than and tm == newer_than) or \
                    (tm > newer_than and tm < older_than):
                database = self._db_name + '_' + snapshot
                logger.info('Dropping database ' + database)
                self._mysql.query('DROP DATABASE ' + database)

    def _do_list_snapshots(self):
        databases = self._mysql.query('SHOW DATABASES')

        snapshots = [db.replace(self._db_name + '_', '') for db in databases if db.startswith(self._db_name + '_')]

        return sorted(snapshots, reverse = True)

    def _do_recover_from(self, timestamp): #override
        snapshot_name = self._db_name + '_' + timestamp

        tables = self._mysql.query('SHOW TABLES')

        for table in tables:
            self._mysql.query('TRUNCATE TABLE `%s`.`%s`' % (self._db_name, table))
            self._mysql.query('INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`' % (self._db_name, table, snapshot_name, table))

    def _do_switch_snapshot(self, timestamp): #override
        snapshot_name = self._db_name + '_' + timestamp

        self._mysql.query('USE ' + snapshot_name)

    def _do_set_last_update(self, tm): #override
        self._mysql.query('UPDATE `system` SET `last_update` = FROM_UNIXTIME(%d)' % int(tm))
        self.last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

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
        # Load sites
        site_list = []

        sites = self._mysql.query('SELECT `name`, `host`, `storage_type`, `backend`, `storage`, `cpu`, `status` FROM `sites`')

        logger.info('Loaded data for %d sites.', len(sites))
        
        for name, host, storage_type, backend, storage, cpu, status in sites:
            if type(site_filt) is str:
                if site_filt != '*' and not fnmatch.fnmatch(name, site_filt):
                    continue

            elif type(site_filt) is list:
                if name not in site_filt:
                    continue

            site = Site(name, host = host, storage_type = Site.storage_type_val(storage_type), backend = backend, storage = storage, cpu = cpu, status = status)
            site_list.append(site)

        self._set_site_ids(site_list)

        mean_storage = sum([s.storage for s in site_list]) / len(filter(lambda s: s.storage != 0., site_list))
        mean_cpu = sum([s.cpu for s in site_list]) / len(filter(lambda s: s.cpu != 0., site_list))

        for site in site_list:
            if site.storage == 0.:
                logger.info('Setting storage for %s to mean %f', site.name, mean_storage)
                site.storage = mean_storage

            if site.cpu == 0.:
                logger.info('Setting CPU for %s to mean %f', site.name, mean_cpu)
                site.cpu = mean_cpu

        # Load groups
        group_list = []

        groups = self._mysql.query('SELECT `name` FROM `groups`')

        logger.info('Loaded data for %d groups.', len(groups))

        for name in groups:
            group = Group(name)
            group_list.append(group)

        self._set_group_ids(group_list)

        # Load site quotas
        quotas = self._mysql.query('SELECT `site_id`, `group_id`, `storage` FROM `quotas`')
        for site_id, group_id, storage in quotas:
            try:
                site = self._ids_to_sites[site_id]
            except KeyError:
                continue

            try:
                group = self._ids_to_groups[group_id]
            except KeyError:
                continue

            site.group_quota[group] = storage

        for site in site_list:
            for group in group_list:
                if group not in site.group_quota:
                    logger.info('Setting quota for %s on %s to %f', group.name, site.name, site.storage / len(group_list))
                    site.group_quota[group] = site.storage / len(group_list)

        # Load software versions
        software_version_map = {} # id -> version

        versions = self._mysql.query('SELECT `id`, `cycle`, `major`, `minor`, `suffix` FROM `software_versions`')

        logger.info('Loaded data for %d software versions.', len(versions))

        for software_version_id, cycle, major, minor, suffix in versions:
            software_version_map[software_version_id] = (cycle, major, minor, suffix)

        # Load datasets
        dataset_list = []

        datasets = self._mysql.query('SELECT `name`, `size`, `num_files`, `is_open`, `status`+0, `on_tape`, `data_type`+0, `software_version_id`, UNIX_TIMESTAMP(`last_update`) FROM `datasets`')

        logger.info('Loaded data for %d datasets.', len(datasets))

        for name, size, num_files, is_open, status, on_tape, data_type, software_version_id, last_update in datasets:
            if dataset_filt != '/*/*/*' and not fnmatch.fnmatch(name, dataset_filt):
                continue

            dataset = Dataset(name, size = size, num_files = num_files, is_open = is_open, status = int(status), on_tape = on_tape, data_type = int(data_type), last_update = last_update)
            if software_version_id != 0:
                dataset.software_version = software_version_map[software_version_id]

            dataset_list.append(dataset)

        self._set_dataset_ids(dataset_list)

        if len(dataset_list) == 0:
            return site_list, group_list, dataset_list

        # Load blocks
        block_map = {} # id -> block

        sql = 'SELECT `id`, `dataset_id`, `name`, `size`, `num_files`, `is_open` FROM `blocks`'
        if dataset_filt != '/*/*/*':
            sql += ' WHERE `dataset_id` IN (%s)' % (','.join(map(str, self._ids_to_datasets.keys())))

        blocks = self._mysql.query(sql)

        logger.info('Loaded data for %d blocks.', len(blocks))

        for block_id, dataset_id, name, size, num_files, is_open in blocks:
            block = Block(name, size = size, num_files = num_files, is_open = is_open)

            dataset = self._ids_to_datasets[dataset_id]
            block.dataset = dataset
            dataset.blocks.append(block)

            block_map[block_id] = block

        if load_replicas:
            # Link datasets to sites
            logger.info('Linking datasets to sites.')
    
            sql = 'SELECT `dataset_id`, `site_id`, `group_id`, `is_complete`, `is_partial`, `is_custodial` FROM `dataset_replicas`'
    
            conditions = []
            if site_filt != '*':
                conditions.append('`site_id` IN (%s)' % (','.join(map(str, self._ids_to_sites.keys()))))
            if dataset_filt != '/*/*/*':
                conditions.append('`dataset_id` IN (%s)' % (','.join(map(str, self._ids_to_datasets.keys()))))
    
            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)
    
            dataset_replicas = self._mysql.query(sql)
    
            for dataset_id, site_id, group_id, is_complete, is_partial, is_custodial in dataset_replicas:
                dataset = self._ids_to_datasets[dataset_id]
                site = self._ids_to_sites[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = self._ids_to_groups[group_id]
    
                rep = DatasetReplica(dataset, site, group = group, is_complete = is_complete, is_partial = is_partial, is_custodial = is_custodial)
    
                dataset.replicas.append(rep)
                site.dataset_replicas.append(rep)
    
            logger.info('Linking blocks to sites.')
    
            # Link blocks to sites and groups
            sql = 'SELECT `block_id`, `site_id`, `group_id`, `is_complete`, `is_custodial` FROM `block_replicas`'
    
            conditions = []
            if site_filt != '*':
                conditions.append('`site_id` IN (%s)' % (','.join(map(str, self._ids_to_sites.keys()))))
            if dataset_filt != '/*/*/*':
                conditions.append('`block_id` IN (%s)' % (','.join(map(str, block_map.keys()))))
    
            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)
    
            block_replicas = self._mysql.query(sql)
    
            for block_id, site_id, group_id, is_complete, is_custodial in block_replicas:
                block = block_map[block_id]
                site = self._ids_to_sites[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = self._ids_to_groups[group_id]
    
                rep = BlockReplica(block, site, group = group, is_complete = is_complete, is_custodial = is_custodial)
    
                block.replicas.append(rep)
                site.block_replicas.append(rep)
    
                dataset_replica = block.dataset.find_replica(site)
                if dataset_replica:
                    dataset_replica.block_replicas.append(rep)
                else:
                    logger.warning('Found a block replica %s:%s#%s without a corresponding dataset replica', site.name, block.dataset.name, block.name)
    
            # For datasets with all replicas complete and not partial, block replica data is not saved on disk
            for dataset in dataset_list:
                for replica in dataset.replicas:
                    if len(replica.block_replicas) != 0:
                        # block replicas of this dataset replica is already taken care of above
                        continue
    
                    for block in dataset.blocks:
                        rep = BlockReplica(block, replica.site, group = replica.group, is_complete = True, is_custodial = replica.is_custodial)
                        block.replicas.append(rep)
                        replica.site.block_replicas.append(rep)
                        replica.block_replicas.append(rep)

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
        # old accesses will be removed automatically next time the access information is saved from memory
        accesses = self._mysql.query('SELECT `dataset_id`, `site_id`, YEAR(`date`), MONTH(`date`), DAY(`date`), `access_type`+0, `num_accesses`, `cputime` FROM `dataset_accesses` WHERE `date` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `site_id`, `date`')

        last_update = datetime.date.min

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

            if date > last_update:
                last_update = date

        logger.info('Loaded %d replica access data. Last update on %s UTC', len(accesses), last_update.strftime('%Y-%m-%d'))

        return last_update

    def _do_load_dataset_requests(self, datasets): #override
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(datasets)

        for dataset in datasets:
            dataset.requests = []

        # pick up requests that are less than 1 year old
        # old requests will be removed automatically next time the access information is saved from memory
        requests = self._mysql.query('SELECT `id`, `dataset_id`, UNIX_TIMESTAMP(`queue_time`), UNIX_TIMESTAMP(`completion_time`), `nodes_total`, `nodes_done`, `nodes_failed`, `nodes_queued` FROM `dataset_requests` WHERE `queue_time` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `queue_time`')

        last_update = datetime.datetime.min

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

            if update > last_update:
                last_update = update

        logger.info('Loaded %d dataset request data. Last update on %s UTC', len(requests), last_update.strftime('%Y-%m-%d %H:%M:%S'))

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

        self._mysql.insert_many('groups', ('name',), lambda g: (g.name,), groups)

        # group_ids map not used here but needs to be reloaded
        self._set_group_ids(groups)

    def _do_save_datasets(self, datasets): #override
        # insert/update software versions
        # first, make the list of unique software versions (excluding defualt (0,0,0,''))
        version_list = list(set([d.software_version for d in datasets if d.software_version[0] != 0]))
        logger.info('Inserting/updating %d software versions.', len(version_list))

        fields = ('cycle', 'major', 'minor', 'suffix')

        self._mysql.insert_many('software_versions', fields, lambda v: v, version_list) # version is already a tuple

        version_map = {(0, 0, 0, ''): 0} # tuple -> id
        versions = self._mysql.query('SELECT `id`, `cycle`, `major`, `minor`, `suffix` FROM `software_versions`')

        for version_id, cycle, major, minor, suffix in versions:
            version_map[(cycle, major, minor, suffix)] = version_id

        # insert/update datasets
        # since the dataset list can be large, it is faster to recreate the entire table than to update and clean.
        logger.info('Inserting/updating %d datasets.', len(datasets))

        if len(self._datasets_to_ids) == 0:
            # load up the latest dataset ids
            self._set_dataset_ids(datasets)

        self._mysql.query('CREATE TABLE `datasets_new` LIKE `datasets`')

        # first insert known datasets
        known_datasets = []
        new_datasets = []
        for dataset in datasets:
            try:
                known_datasets.append((dataset, self._datasets_to_ids[dataset]))
            except KeyError:
                new_datasets.append(dataset)

        fields = ('id', 'name', 'size', 'num_files', 'is_open', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update')
        # MySQL expects the local time for last_update
        mapping = lambda (d, i): (
            i,
            d.name,
            d.size,
            d.num_files,
            d.is_open,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update))
        )

        self._mysql.insert_many('datasets_new', fields, mapping, known_datasets, do_update = False)

        fields = ('name', 'size', 'num_files', 'is_open', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update')
        # MySQL expects the local time for last_update
        mapping = lambda d: (
            d.name,
            d.size,
            d.num_files,
            d.is_open,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update))
        )

        self._mysql.insert_many('datasets_new', fields, mapping, new_datasets, do_update = False)

        self._mysql.query('RENAME TABLE `datasets` TO `datasets_old`')
        self._mysql.query('RENAME TABLE `datasets_new` TO `datasets`')
        self._mysql.query('DROP TABLE `datasets_old`')

        # reload the dataset ids
        self._set_dataset_ids(datasets)

        # insert/update blocks
        # since the block list can be large, it is faster to recreate the entire table than to update and clean.
        all_blocks = sum([d.blocks for d in datasets], [])

        logger.info('Inserting/updating %d blocks.', len(all_blocks))

        self._mysql.query('CREATE TABLE `blocks_new` LIKE `blocks`')

        fields = ('name', 'dataset_id', 'size', 'num_files', 'is_open')
        mapping = lambda b: (b.name, self._datasets_to_ids[b.dataset], b.size, b.num_files, b.is_open)

        self._mysql.insert_many('blocks_new', fields, mapping, all_blocks, do_update = False)

        self._mysql.query('RENAME TABLE `blocks` TO `blocks_old`')
        self._mysql.query('RENAME TABLE `blocks_new` TO `blocks`')
        self._mysql.query('DROP TABLE `blocks_old`')

        # at this point the block_ids may have changed
        # truncate block_replicas table to avoid inconsistencies
        self._mysql.query('TRUNCATE TABLE `block_replicas`')

    def _do_save_replicas(self, all_replicas): #override
        # make name -> id maps for use later
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(list(set([r.dataset for r in all_replicas])))
        if len(self._sites_to_ids) == 0:
            self._set_site_ids(list(set([r.site for r in all_replicas])))
        if len(self._groups_to_ids) == 0:
            self._group_ids(list(set([r.group for g in all_replicas])))

        # insert/update dataset replicas
        logger.info('Inserting/updating %d dataset replicas.', len(all_replicas))

        self._mysql.query('CREATE TABLE `dataset_replicas_new` LIKE `dataset_replicas`')

        fields = ('dataset_id', 'site_id', 'group_id', 'is_complete', 'is_partial', 'is_custodial')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_partial, r.is_custodial)

        self._mysql.insert_many('dataset_replicas_new', fields, mapping, all_replicas, do_update = False)

        self._mysql.query('RENAME TABLE `dataset_replicas` TO `dataset_replicas_old`')
        self._mysql.query('RENAME TABLE `dataset_replicas_new` TO `dataset_replicas`')
        self._mysql.query('DROP TABLE `dataset_replicas_old`')

        # insert/update block replicas for non-complete dataset replicas
        all_block_replicas = []

        # loop over the replicas but visit each dataset only once
        datasets = set()

        for replica in all_replicas:
            dataset = replica.dataset

            if dataset in datasets:
                continue

            datasets.add(dataset)

            dataset_id = self._datasets_to_ids[dataset]

            need_blocklevel = []
            for replica in dataset.replicas:
                # replica is not complete
                if replica.is_partial or not replica.is_complete:
                    need_blocklevel.append(replica)
                    continue

                # replica has multiple owners
                for block_replica in replica.block_replicas:
                    if block_replica.group != replica.group:
                        need_blocklevel.append(replica)
                        break

            if len(need_blocklevel) != 0:
                logger.info('Not all replicas of %s is complete. Saving block info.', dataset.name)
                block_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id))

            for replica in dataset.replicas:
                site = replica.site
                site_id = self._sites_to_ids[site]

                if replica not in need_blocklevel:
                    # this is a complete replica. Remove block replica for this dataset replica.
                    self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id), additional_conditions = ['`site_id` = %d' % site_id])

                    continue

                # add the block replicas on this site to block_replicas together with SQL ID
                all_block_replicas += [(r, block_ids[r.block.name]) for r in replica.block_replicas]

        self._mysql.query('CREATE TABLE `block_replicas_new` LIKE `block_replicas`')

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda (r, bid): (bid, self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_custodial)

        self._mysql.insert_many('block_replicas_new', fields, mapping, all_block_replicas, do_update = False)

        self._mysql.query('RENAME TABLE `block_replicas` TO `block_replicas_old`')
        self._mysql.query('RENAME TABLE `block_replicas_new` TO `block_replicas`')
        self._mysql.query('DROP TABLE `block_replicas_old`')

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

    def _do_add_dataset_replicas(self, replicas): #override
        # make name -> id maps for use later
        if len(self._datasets_to_ids) == 0 or len(self._sites_to_ids) == 0 or len(self._groups_to_ids) == 0:
            raise RuntimeError('add_dataset_replicas cannot be called before initializing the id maps')

        # insert/update dataset replicas
        logger.info('Inserting/updating %d dataset replicas.', len(replicas))

        fields = ('dataset_id', 'site_id', 'group_id', 'is_complete', 'is_partial', 'is_custodial')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_partial, r.is_custodial)

        self._mysql.insert_many('dataset_replicas', fields, mapping, replicas)

        # insert/update block replicas for non-complete dataset replicas
        all_block_replicas = []

        for replica in replicas:
            dataset_id = self._datasets_to_ids[replica.dataset]
            site_id = self._sites_to_ids[replica.site]
            
            if not replica.is_partial and replica.is_complete:
                # this is a complete replica. Remove block replica for this dataset replica.
                self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id), additional_conditions = ['`site_id` = %d' % site_id])
                continue

            block_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id))

            # add the block replicas on this site to block_replicas together with SQL ID
            all_block_replicas += [(r, block_ids[r.block.name]) for r in replica.block_replicas]

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda (r, bid): (bid, self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_custodial)

        self._mysql.insert_many('block_replicas', fields, mapping, all_block_replicas)

    def _do_delete_dataset(self, dataset): #override
        self._mysql.query('DELETE FROM `datasets` WHERE `name` LIKE %s', dataset.name)

    def _do_delete_block(self, block): #override
        self._mysql.query('DELETE FROM `blocks` WHERE `name` LIKE %s', block.name)

    def _do_delete_datasetreplicas(self, site, datasets, delete_blockreplicas): #override
        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = self._mysql.select_many('datasets', 'id', 'name', ['\'%s\'' % d.name for d in datasets])

        self._mysql.delete_in('dataset_replicas', 'dataset_id', dataset_ids, 'site_id = %d' % site_id)

        if delete_blockreplicas:
            self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', 'dataset_id', dataset_ids), 'site_id = %d' % site_id)

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

        sql = 'DELETE FROM `block_replicas` AS replicas'
        sql += ' INNER JOIN `blocks` ON `blocks`.`id` = replicas.`block_id`'
        sql += ' WHERE (replicas.`site_id`, `blocks`.`dataset_id`, `blocks`.`name`) IN ({combinations})'

        combinations = ','.join(['(%d,%d,\'%s\')' % (site_ids[r.site], dataset_ids[r.block.dataset], r.block.name) for r in replica_list])

        self._mysql.query(sql.format(combinations = combinations))

    def _do_close_block(self, dataset_name, block_name): #override
        self._mysql.query('UPDATE `blocks` INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id` SET `blocks`.`is_open` = 0 WHERE `datasets`.`name` LIKE %s AND `blocks`.`name` LIKE %s', dataset_name, block_name)

    def _do_set_dataset_status(self, dataset_name, status_str): #override
        self._mysql.query('UPDATE `datasets` SET `status` = %s WHERE `name` LIKE %s', status_str, dataset_name)

    def _set_dataset_ids(self, datasets):
        # reset id maps to the current content in the DB.

        logger.debug('set_dataset_ids')

        self._datasets_to_ids = {}
        self._ids_to_datasets = {}

        name_map = dict([d.name, d] for d in datasets)

        ids_source = self._mysql.query('SELECT `name`, `id` FROM `datasets`')
        for name, dataset_id in ids_source:
            try:
                dataset = name_map[name]
            except KeyError:
                continue

            self._datasets_to_ids[dataset] = dataset_id
            self._ids_to_datasets[dataset_id] = dataset

    def _set_site_ids(self, sites):
        # reset id maps to the current content in the DB.

        self._sites_to_ids = {}
        self._ids_to_sites = {}

        ids_source = self._mysql.query('SELECT `name`, `id` FROM `sites`')
        for name, site_id in ids_source:
            try:
                site = next(d for d in sites if d.name == name)
            except StopIteration:
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
                group = next(d for d in groups if d.name == name)
            except StopIteration:
                continue

            self._groups_to_ids[group] = group_id
            self._ids_to_groups[group_id] = group
