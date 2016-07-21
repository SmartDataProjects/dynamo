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
        # Load sites
        site_list = []

        sites = self._mysql.query('SELECT `name`, `host`, `storage_type`, `backend`, `storage`, `cpu`, `status` FROM `sites`')

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

        logger.info('Loaded data for %d sites.', len(sites))

#        mean_storage = sum([s.storage for s in site_list]) / len(filter(lambda s: s.storage != 0., site_list))
#        mean_cpu = sum([s.cpu for s in site_list]) / len(filter(lambda s: s.cpu != 0., site_list))
        mean_storage = 500
        mean_cpu = 2.

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

        for name in groups:
            group = Group(name)
            group_list.append(group)

        self._set_group_ids(group_list)

        logger.info('Loaded data for %d groups.', len(groups))

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
#                if site.group_present(group)
#                    logger.info('Setting quota for %s on %s to %d', group.name, site.name, int(site.storage / len(group_list)))
#                    site.set_group_quota(group, int(site.storage / len(group_list)))

        # Load software versions
        software_version_map = {} # id -> version

        versions = self._mysql.query('SELECT `id`, `cycle`, `major`, `minor`, `suffix` FROM `software_versions`')

        for software_version_id, cycle, major, minor, suffix in versions:
            software_version_map[software_version_id] = (cycle, major, minor, suffix)

        logger.info('Loaded data for %d software versions.', len(versions))

        del versions

        # Load datasets
        dataset_list = []

        datasets = self._mysql.query('SELECT `name`, `status`+0, `on_tape`, `data_type`+0, `software_version_id`, UNIX_TIMESTAMP(`last_update`), `is_open` FROM `datasets`')

        for name, status, on_tape, data_type, software_version_id, last_update, is_open in datasets:
            if dataset_filt != '/*/*/*' and not fnmatch.fnmatch(name, dataset_filt):
                continue

            dataset = Dataset(name, status = int(status), on_tape = on_tape, data_type = int(data_type), last_update = last_update, (is_open == 1))
            if software_version_id != 0:
                dataset.software_version = software_version_map[software_version_id]

            dataset_list.append(dataset)

        self._set_dataset_ids(dataset_list)

        logger.info('Loaded data for %d datasets.', len(datasets))

        del datasets

        if len(dataset_list) == 0:
            return site_list, group_list, dataset_list

        # Load blocks
        block_map = {} # id -> block

        sql = 'SELECT `id`, `dataset_id`, `name`, `size`, `num_files`, `is_open` FROM `blocks`'
        if dataset_filt != '/*/*/*':
            sql += ' WHERE `dataset_id` IN (%s)' % (','.join(map(str, self._ids_to_datasets.keys())))
        sql += ' ORDER BY `dataset_id`'

        blocks = self._mysql.query(sql)

        _dataset_id = 0
        dataset = None
        for block_id, dataset_id, name, size, num_files, is_open in blocks:
            if dataset_id != _dataset_id:
                dataset = self._ids_to_datasets[dataset_id]
                _dataset_id = dataset_id

            block = Block(Block.translate_name(name), dataset, size, num_files, is_open)

            dataset.blocks.append(block)

            block_map[block_id] = block

        logger.info('Loaded data for %d blocks.', len(blocks))

        del blocks

        if load_replicas:
            # Link datasets to sites
            logger.info('Linking datasets to sites.')
    
            sql = 'SELECT `dataset_id`, `site_id`, `group_id`, `completion`, `is_custodial`, UNIX_TIMESTAMP(`last_block_created`) FROM `dataset_replicas`'
    
            conditions = []
            if site_filt != '*':
                conditions.append('`site_id` IN (%s)' % (','.join(map(str, self._ids_to_sites.keys()))))
            if dataset_filt != '/*/*/*':
                conditions.append('`dataset_id` IN (%s)' % (','.join(map(str, self._ids_to_datasets.keys()))))
    
            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)

            sql += 'ORDER BY `dataset_id`'

            dataset_replicas = self._mysql.query(sql)

            _dataset_id = 0
    
            for dataset_id, site_id, group_id, completion, is_custodial, last_block_created in dataset_replicas:
                if dataset_id != _dataset_id:
                    _dataset_id = dataset_id
                    dataset = self._ids_to_datasets[_dataset_id]

                site = self._ids_to_sites[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = self._ids_to_groups[group_id]
    
                rep = DatasetReplica(dataset, site, group = group, is_complete = (completion != 'incomplete'), is_custodial = is_custodial, last_block_created = last_block_created)
    
                dataset.replicas.append(rep)
                site.dataset_replicas.append(rep)

            del dataset_replicas

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

            sql += 'ORDER BY `block_id`, `site_id`'

            block_replicas = self._mysql.query(sql)

            sql = 'SELECT `block_id`, `site_id`, `size` FROM `block_replica_sizes`'

            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)

            sql += 'ORDER BY `block_id`, `site_id`'

            block_replica_sizes = self._mysql.query(sql)

            _block_id = 0
            isize = 0
            nsize = len(block_replica_sizes)
    
            for block_id, site_id, group_id, is_complete, is_custodial in block_replicas:
                # find the block: avoid looking up for each entry by ordering the block_replicas list by block_id
                if block_id != _block_id:
                    _block_id = block_id
                    block = block_map[_block_id]

                # find the site
                site = self._ids_to_sites[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = self._ids_to_groups[group_id]

                size = block.size
                # find the physical size for incomplete replicas
                if not is_complete:
                    # fast forward the size list to this block (in principle nothing should happen here if the sizes table is up to date)
                    while isize < nsize and block_replica_sizes[isize][0] < _block_id:
                        isize += 1

                    # this replica is incomplete -> there must be a matching entry..
                    if isize < nsize and block_replica_sizes[isize][0] == _block_id:
                        while isize < nsize and block_replica_sizes[isize][1] < site_id:
                            isize += 1
    
                        if isize < nsize and block_replica_sizes[isize][1] == site_id:
                            size = block_replica_sizes[isize][2]
                            isize += 1
    
                rep = BlockReplica(block, site, group = group, is_complete = is_complete, is_custodial = is_custodial, size = size)
    
                site.add_block_replica(rep)
    
                dataset_replica = block.dataset.find_replica(site)
                if dataset_replica:
                    dataset_replica.block_replicas.append(rep)
                else:
                    logger.warning('Found a block replica %s:%s#%s without a corresponding dataset replica', site.name, block.dataset.name, block.real_name())

            del block_replicas

            # For datasets with all replicas complete and not partial, block replica data is not saved on disk
            for dataset in dataset_list:
                for replica in dataset.replicas:
                    if len(replica.block_replicas) != 0:
                        # block replicas of this dataset replica is already taken care of above
                        continue
    
                    for block in dataset.blocks:
                        rep = BlockReplica(block, replica.site, group = replica.group, is_complete = True, is_custodial = replica.is_custodial, size = block.size)
                        replica.site.add_block_replica(rep)
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
        # old accesses will eb removed automatically next time the access information is saved from memory
        accesses = self._mysql.query('SELECT `dataset_id`, `site_id`, YEAR(`date`), MONTH(`date`), DAY(`date`), `access_type`+0, `num_accesses`, `cputime` FROM `dataset_accesses` WHERE `date` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `site_id`, `date`')

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

        del version_list

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

        # separate the datasets into new and known (have id in table)
        known_datasets = []
        new_datasets = []
        for dataset in datasets:
            try:
                dataset_id = self._datasets_to_ids[dataset]
            except KeyError:
                new_datasets.append(dataset)
            else:
                known_datasets.append((dataset, dataset_id))

        # datasets.size stored only for query speedup in inventory web interface
        fields = ('id', 'name', 'size', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda (d, i): (
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
        
        self._mysql.insert_many('datasets_new', fields, mapping, known_datasets, do_update = False)

        del known_datasets

        # at this point we should drop records that make reference to datasets that are not in the store any more
        self._mysql.delete_not_in('dataset_replicas', 'dataset_id', ('id', 'datasets_new'))
        self._mysql.delete_not_in('dataset_accesses', 'dataset_id', ('id', 'datasets_new'))
        self._mysql.delete_not_in('dataset_requests', 'dataset_id', ('id', 'datasets_new'))

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

        self._mysql.insert_many('datasets_new', fields, mapping, new_datasets, do_update = False)

        self._mysql.query('RENAME TABLE `datasets` TO `datasets_old`')
        self._mysql.query('RENAME TABLE `datasets_new` TO `datasets`')
        self._mysql.query('DROP TABLE `datasets_old`')

        # reload the dataset ids
        self._set_dataset_ids(new_datasets, update = True)

        del new_datasets

        # insert/update blocks
        all_blocks = []
        for dataset in datasets:
            all_blocks.extend(dataset.blocks)

        logger.info('Inserting/updating %d blocks.', len(all_blocks))

        self._mysql.query('CREATE TABLE `blocks_new` LIKE `blocks`')

        fields = ('name', 'dataset_id', 'size', 'num_files', 'is_open')
        mapping = lambda b: (
            b.real_name(),
            self._datasets_to_ids[b.dataset],
            b.size,
            b.num_files,
            1 if b.is_open else 0
        )

        self._mysql.insert_many('blocks_new', fields, mapping, all_blocks, do_update = False)

        del all_blocks

        self._mysql.query('RENAME TABLE `blocks` TO `blocks_old`')
        self._mysql.query('RENAME TABLE `blocks_new` TO `blocks`')
        self._mysql.query('DROP TABLE `blocks_old`')

        # at this point the block_ids may have changed
        # truncate block_replicas and block_replica_sizes tables to avoid inconsistencies
        self._mysql.query('TRUNCATE TABLE `block_replicas`')
        self._mysql.query('TRUNCATE TABLE `block_replica_sizes`')

    def _do_save_replicas(self, sites, groups, datasets): #override
        # make name -> id maps for use later
        if len(self._datasets_to_ids) == 0:
            self._set_dataset_ids(datasets)
        if len(self._sites_to_ids) == 0:
            self._set_site_ids(sites)
        if len(self._groups_to_ids) == 0:
            self._group_ids(groups)

        # insert/update dataset replicas
        logger.info('Inserting/updating dataset replicas.')

        self._mysql.query('CREATE TABLE `dataset_replicas_new` LIKE `dataset_replicas`')

        fields = ('dataset_id', 'site_id', 'group_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        all_replicas = []
        for dataset in datasets:
            all_replicas.extend(dataset.replicas)

        self._mysql.insert_many('dataset_replicas_new', fields, mapping, all_replicas, do_update = False)

        self._mysql.query('RENAME TABLE `dataset_replicas` TO `dataset_replicas_old`')
        self._mysql.query('RENAME TABLE `dataset_replicas_new` TO `dataset_replicas`')
        self._mysql.query('DROP TABLE `dataset_replicas_old`')

        # insert/update block replicas for non-complete dataset replicas
        blockreps_to_write = []
        blockrepsizes_to_write = []

        _dataset = None
        for replica in all_replicas:
            if replica.dataset != _dataset:
                dataset_id = self._datasets_to_ids[replica.dataset]
                _dataset = replica.dataset

            # replica is incomplete or has multiple owners
            if not replica.is_full() or replica.group is None:
                blockreps_to_write.extend([(dataset_id, r) for r in replica.block_replicas])
                blockrepsizes_to_write.extend([(r, r.size) for r in replica.block_replicas if not r.is_complete])

        del all_replicas

        logger.info('Saving %d block replica info.', len(blockreps_to_write))

        block_to_id = {}

        block_data = self._mysql.select_many('blocks', ('dataset_id', 'name', 'id'), ('dataset_id', 'name'), ['(%d,\'%s\')' % (did, r.block.real_name()) for did, r in blockreps_to_write], order_by = 'dataset_id')

        _dataset_id = 0
        for dataset_id, block_name, block_id in block_data:
            if dataset_id != _dataset_id:
                dataset = self._ids_to_datasets[dataset_id]
                _dataset_id = dataset_id

            block = dataset.find_block(Block.translate_name(block_name))
            block_to_id[block] = block_id

        del block_data

        self._mysql.query('CREATE TABLE `block_replicas_new` LIKE `block_replicas`')

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda (did, r): (block_to_id[r.block], self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_custodial)

        self._mysql.insert_many('block_replicas_new', fields, mapping, blockreps_to_write, do_update = False)

        del blockreps_to_write

        self._mysql.query('RENAME TABLE `block_replicas` TO `block_replicas_old`')
        self._mysql.query('RENAME TABLE `block_replicas_new` TO `block_replicas`')
        self._mysql.query('DROP TABLE `block_replicas_old`')

        self._mysql.query('CREATE TABLE `block_replica_sizes_new` LIKE `block_replica_sizes`')

        fields = ('block_id', 'site_id', 'size')
        mapping = lambda (r, size): (block_to_id[r.block], self._sites_to_ids[r.site], size)

        self._mysql.insert_many('block_replica_sizes_new', fields, mapping, blockrepsizes_to_write, do_update = False)

        del blockrepsizes_to_write

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

        fields = ('dataset_id', 'site_id', 'group_id', 'completion', 'is_custodial', 'last_block_created')
        mapping = lambda r: (self._datasets_to_ids[r.dataset], self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, 'partial' if r.is_partial() else ('full' if r.is_complete else 'incomplete'), r.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.last_block_created)))

        self._mysql.insert_many('dataset_replicas', fields, mapping, replicas)

        # insert/update block replicas for non-complete dataset replicas
        all_block_replicas = []

        for replica in replicas:
            dataset_id = self._datasets_to_ids[replica.dataset]
            site_id = self._sites_to_ids[replica.site]
            
            if replica.is_full() and replica.group is not None:
                # this is a complete replica. Remove block replica for this dataset replica.
                self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id), additional_conditions = ['`site_id` = %d' % site_id])
                self._mysql.delete_in('block_replica_sizes', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id), additional_conditions = ['`site_id` = %d' % site_id])
                continue

            block_ids = {}
            for name_str, block_id in self._mysql.query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id):
                block_ids[Block.translate_name(name_str)] = block_id

            # add the block replicas on this site to block_replicas together with SQL ID
            all_block_replicas.extend([(r, block_ids[r.block.name]) for r in replica.block_replicas])

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial')
        mapping = lambda (r, bid): (bid, self._sites_to_ids[r.site], self._groups_to_ids[r.group] if r.group else 0, r.is_complete, r.is_custodial)

        self._mysql.insert_many('block_replicas', fields, mapping, all_block_replicas)

        fields = ('block_id', 'site_id', 'size')
        mapping = lambda (r, bid): (bid, self._sites_to_ids[r.site], r.size)

        self._mysql.insert_many('block_replica_sizes', fields, mapping, [entry for entry in all_block_replicas if not entry[0].is_complete])

    def _do_delete_dataset(self, dataset): #override
        """
        Delete everything related to this dataset
        """
        try:
            dataset_id = self._mysql.query('SELECT `id` FROM `datasets` WHERE `name` LIKE %s', dataset.name)[0]
        except IndexError:
            return

        self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id))
        self._mysql.delete_in('block_replica_sizes', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id))
        self._mysql.query('DELETE FROM `blocks` WHERE `dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM `dataset_replicas` WHERE `dataset_id` = %s', dataset_id)
        self._mysql.query('DELETE FROM `datasets` WHERE `id` = %s', dataset_id)

    def _do_delete_block(self, block): #override
        try:
            block_id = self._mysql.query('SELECT `id` FROM `blocks` WHERE `name` LIKE %s AND `dataset_id` IN (SELECT `id` FROM `datasets` WHERE `name` LIKE %s)', block.real_name(), block.dataset.name)[0]
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
            self._mysql.delete_in('block_replicas', 'block_id', ('id', 'blocks', 'dataset_id', dataset_ids), additional_conditions = ['site_id = %d' % site_id])
            self._mysql.delete_in('block_replica_sizes', 'block_id', ('id', 'blocks', 'dataset_id', dataset_ids), additional_conditions = ['site_id = %d' % site_id])

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

            sql = 'DELETE FROM `block_replicas` WHERE `site_id` = %d' % site_id
            sql += ' AND `block_id` IN'
            sql += ' (SELECT `id` FROM `blocks` WHERE `dataset_id` IN (%s))' % ','.join(['%d' % dataset_ids[r.block.dataset] for r in replicas_on_site])

            self._mysql.query(sql)

            sql = 'DELETE FROM `block_replica_sizes` WHERE `site_id` = %d' % site_id
            sql += ' AND `block_id` IN'
            sql += ' (SELECT `id` FROM `blocks` WHERE `dataset_id` IN (%s))' % ','.join(['%d' % dataset_ids[r.block.dataset] for r in replicas_on_site])

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
                group = next(d for d in groups if d.name == name)
            except StopIteration:
                continue

            self._groups_to_ids[group] = group_id
            self._ids_to_groups[group_id] = group
