import os
import time
import re
import socket
import logging
import fnmatch
import MySQLdb

from common.interface.inventory import InventoryInterface
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
import common.configuration as config

logger = logging.getLogger(__name__)

class MySQLInterface(InventoryInterface):
    """Interface to MySQL."""

    class DatabaseError(Exception):
        pass

    def __init__(self):
        super(MySQLInterface, self).__init__()

        self._db_params = {'host': config.mysql.host, 'user': config.mysql.user, 'passwd': config.mysql.passwd, 'db': config.mysql.db}
        self._current_db = self._db_params['db']

        self.connection = MySQLdb.connect(**self._db_params)

        self.last_update = self._query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

    def _do_acquire_lock(self): #override
        while True:
            # Use the system table to "software-lock" the database
            self._query('LOCK TABLES `system` WRITE')
            self._query('UPDATE `system` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', socket.gethostname(), os.getpid())

            # Did the update go through?
            host, pid = self._query('SELECT `lock_host`, `lock_process` FROM `system`')[0]
            self._query('UNLOCK TABLES')

            if host == socket.gethostname() and pid == os.getpid():
                # The database is locked.
                break

            logger.warning('Failed to lock database. Waiting 30 seconds..')

            time.sleep(30)

    def _do_release_lock(self): #override
        self._query('LOCK TABLES `system` WRITE')
        self._query('UPDATE `system` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._query('SELECT `lock_host`, `lock_process` FROM `system`')[0]
        self._query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise InventoryInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_make_snapshot(self, clear): #override
        db = self._db_params['db']
        new_db = self._db_params['db'] + time.strftime('_%y%m%d%H%M%S')

        self._query('CREATE DATABASE `%s`' % new_db)

        tables = self._query('SHOW TABLES')

        for table in tables:
            self._query('CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`' % (new_db, table, db, table))
            self._query('INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`' % (new_db, table, db, table))

            if clear == InventoryInterface.CLEAR_ALL or \
               (clear == InventoryInterface.CLEAR_REPLICAS and table in ['dataset_replicas', 'block_replicas']):
                self._query('DROP TABLE `%s`.`%s`' % (db, table))
                self._query('CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`' % (db, table, new_db, table))
       
        last_update = self._query('SELECT `last_update` FROM `%s`.`system`' % db)[0]
        self._query('UPDATE `%s`.`system` SET `lock_host` = \'\', `lock_process` = 0, `last_update` = \'%s\'' % (new_db, last_update))

    def _do_remove_snapshot(self, newer_than, older_than): #override
        snapshots = self._do_list_snapshots()

        print newer_than, older_than
        
        for snapshot in snapshots:
            tm = time.mktime(time.strptime(snapshot, '%y%m%d%H%M%S'))
            if tm >= newer_than and tm <= older_than:
                database = self._db_params['db'] + '_' + snapshot
                logger.info('Dropping database ' + database)
                self._query('DROP DATABASE ' + database)

    def _do_list_snapshots(self):
        databases = self._query('SHOW DATABASES')
        databases.remove('information_schema')
        databases.remove('mysql')
        databases.remove(self._db_params['db'])

        snapshots = [db.replace(self._db_params['db'] + '_', '') for db in databases]

        return sorted(snapshots, reverse = True)

    def _do_switch_snapshot(self, timestamp):
        snapshot_name = self._db_params['db'] + '_' + timestamp

        self._query('USE ' + snapshot_name)

    def _do_timestamp(self, tm): #override
        self._query('UPDATE `system` SET `last_update` = FROM_UNIXTIME(%d)' % int(tm))

    def _do_load_data(self, site_filt, dataset_filt, load_replicas): #override

        # Load sites
        site_list = []
        site_map = {} # id -> site

        sites = self._query('SELECT `id`, `name`, `host`, `storage_type`, `backend`, `capacity`, `used_total` FROM `sites`')

        logger.info('Loaded data for %d sites.', len(sites))
        
        for site_id, name, host, storage_type, backend, capacity, used_total in sites:
            if site_filt != '*' and not fnmatch.fnmatch(name, site_filt):
                continue

            site = Site(name, host = host, storage_type = Site.storage_type_val(storage_type), backend = backend, capacity = capacity, used_total = used_total)
            site_list.append(site)

            site_map[site_id] = site

        # Load groups
        group_list = []
        group_map = {} # id -> group

        groups = self._query('SELECT `id`, `name` FROM `groups`')

        logger.info('Loaded data for %d groups.', len(groups))

        for group_id, name in groups:
            group = Group(name)
            group_list.append(group)

            group_map[group_id] = group

        # Load software versions
        software_version_map = {} # id -> version

        versions = self._query('SELECT `id`, `cycle`, `major`, `minor`, `suffix` FROM `software_versions`')

        logger.info('Loaded data for %d software versions.', len(versions))

        for software_version_id, cycle, major, minor, suffix in versions:
            software_version_map[software_version_id] = (cycle, major, minor, suffix)

        # Load datasets
        dataset_list = []
        dataset_map = {} # id -> site

        datasets = self._query('SELECT `id`, `name`, `size`, `num_files`, `is_open`, `status`+0, `on_tape`, `data_type`+0, `software_version_id` FROM `datasets`')

        logger.info('Loaded data for %d datasets.', len(datasets))

        for dataset_id, name, size, num_files, is_open, status, on_tape, data_type, software_version_id in datasets:
            if dataset_filt != '/*/*/*' and not fnmatch.fnmatch(name, dataset_filt):
                continue

            dataset = Dataset(name, size = size, num_files = num_files, is_open = is_open, status = int(status), on_tape = on_tape, data_type = int(data_type))
            if software_version_id != 0:
                dataset.software_version = software_version_map[software_version_id]

            dataset_list.append(dataset)

            dataset_map[dataset_id] = dataset

        if len(dataset_map) == 0:
            return site_list, group_list, dataset_list

        # Load blocks
        block_map = {} # id -> block

        sql = 'SELECT `id`, `dataset_id`, `name`, `size`, `num_files`, `is_open` FROM `blocks`'
        if dataset_filt != '/*/*/*':
            sql += ' WHERE `dataset_id` IN (%s)' % (','.join(map(str, dataset_map.keys())))

        blocks = self._query(sql)

        logger.info('Loaded data for %d blocks.', len(blocks))

        for block_id, dataset_id, name, size, num_files, is_open in blocks:
            block = Block(name, size = size, num_files = num_files, is_open = is_open)

            dataset = dataset_map[dataset_id]
            block.dataset = dataset
            dataset.blocks.append(block)

            block_map[block_id] = block

        if load_replicas:
            # Link datasets to sites
            logger.info('Linking datasets to sites.')
    
            sql = 'SELECT `dataset_id`, `site_id`, `group_id`, `is_complete`, `is_partial`, `is_custodial` FROM `dataset_replicas`'
    
            conditions = []
            if site_filt != '*':
                conditions.append('`site_id` IN (%s)' % (','.join(map(str, site_map.keys()))))
            if dataset_filt != '/*/*/*':
                conditions.append('`dataset_id` IN (%s)' % (','.join(map(str, dataset_map.keys()))))
    
            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)
    
            dataset_replicas = self._query(sql)
    
            for dataset_id, site_id, group_id, is_complete, is_partial, is_custodial in dataset_replicas:
                dataset = dataset_map[dataset_id]
                site = site_map[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = group_map[group_id]
    
                rep = DatasetReplica(dataset, site, group = group, is_complete = is_complete, is_partial = is_partial, is_custodial = is_custodial)
    
                dataset.replicas.append(rep)
                site.datasets.append(dataset)
    
            logger.info('Linking blocks to sites.')
    
            # Link blocks to sites and groups
            sql = 'SELECT `block_id`, `site_id`, `group_id`, `is_complete`, `is_custodial`, UNIX_TIMESTAMP(`time_created`), UNIX_TIMESTAMP(`time_updated`) FROM `block_replicas`'
    
            conditions = []
            if site_filt != '*':
                conditions.append('`site_id` IN (%s)' % (','.join(map(str, site_map.keys()))))
            if dataset_filt != '/*/*/*':
                conditions.append('`block_id` IN (%s)' % (','.join(map(str, block_map.keys()))))
    
            if len(conditions) != 0:
                sql += ' WHERE ' + ' AND '.join(conditions)
    
            block_replicas = self._query(sql)
    
            for block_id, site_id, group_id, is_complete, is_custodial, time_created, time_updated in block_replicas:
                block = block_map[block_id]
                site = site_map[site_id]
                if group_id == 0:
                    group = None
                else:
                    group = group_map[group_id]
    
                rep = BlockReplica(block, site, group = group, is_complete = is_complete, is_custodial = is_custodial, time_created = time_created, time_updated = time_updated)
    
                block.replicas.append(rep)
                site.blocks.append(block)
    
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
                        replica.site.blocks.append(block)
                        replica.block_replicas.append(rep)

        # Finally set last_update
        self.last_update = self._query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

        # Only the list of sites, groups, and datasets are returned
        return site_list, group_list, dataset_list

    def _do_save_data(self, sites, groups, datasets, clean_stale): #override

        def make_insert_query(table, fields):
            sql = 'INSERT INTO `{table}` ({fields}) VALUES %s'.format(table = table, fields = ','.join(['`%s`' % f for f in fields]))
            sql += ' ON DUPLICATE KEY UPDATE ' + ','.join(['`{f}`=VALUES(`{f}`)'.format(f = f) for f in fields])

            return sql

        # insert/update sites
        logger.info('Inserting/updating %d sites.', len(sites))

        last_id = self._query('SELECT MAX(`id`) FROM `sites`')

        sql = make_insert_query('sites', ['name', 'host', 'storage_type', 'backend', 'capacity', 'used_total'])

        template = '(\'{name}\',\'{host}\',\'{storage_type}\',\'{backend}\',{capacity},{used_total})'
        mapping = lambda s: {'name': s.name, 'host': s.host, 'storage_type': Site.storage_type_name(s.storage_type), 'backend': s.backend, 'capacity': s.capacity, 'used_total': s.used_total}

        self._query_many(sql, template, mapping, sites.values())

        # insert/update groups
        logger.info('Inserting/updating %d groups.', len(groups))

        last_id = self._query('SELECT MAX(`id`) FROM `groups`')

        sql = make_insert_query('groups', ['name'])

        template = '(\'{name}\')'
        mapping = lambda g: {'name': g.name}

        self._query_many(sql, template, mapping, groups.values())

        # insert/update software versions
        # first, make the list of unique software versions (excluding defualt (0,0,0,''))
        version_list = list(set([d.software_version for d in datasets.values() if d.software_version[0] != 0]))
        logger.info('Inserting/updating %d software versions.', len(version_list))

        sql = make_insert_query('software_versions', ['cycle', 'major', 'minor', 'suffix'])

        template = '({cycle},{major},{minor},\'{suffix}\')'
        mapping = lambda v: {'cycle': v[0], 'major': v[1], 'minor': v[2], 'suffix': v[3]}

        self._query_many(sql, template, mapping, version_list)

        version_map = {(0, 0, 0, ''): 0} # tuple -> id
        versions = self._query('SELECT `id`, `cycle`, `major`, `minor`, `suffix` FROM `software_versions`')

        for version_id, cycle, major, minor, suffix in versions:
            version_map[(cycle, major, minor, suffix)] = version_id

        # insert/update datasets
        logger.info('Inserting/updating %d datasets.', len(datasets))

        sql = make_insert_query('datasets', ['name', 'size', 'num_files', 'is_open', 'status', 'on_tape', 'data_type', 'software_version_id'])

        template = '(\'{name}\',{size},{num_files},{is_open},{status},{on_tape},{data_type},{software_version_id})'
        mapping = lambda d: {'name': d.name, 'size': d.size, 'num_files': d.num_files, 'is_open': d.is_open, 'status': d.status, 'on_tape': d.on_tape, 'data_type': d.data_type, 'software_version_id': version_map[d.software_version]}

        self._query_many(sql, template, mapping, datasets.values())

        # make name -> id maps for use later
        site_ids = dict(self._query('SELECT `name`, `id` FROM `sites`'))
        group_ids = dict(self._query('SELECT `name`, `id` FROM `groups`'))
        dataset_ids = dict(self._query('SELECT `name`, `id` FROM `datasets`'))

        # insert/update dataset replicas
        all_replicas = sum([d.replicas for d in datasets.values()], []) # second argument -> start with an empty array and add up

        logger.info('Inserting/updating %d dataset replicas.', len(all_replicas))

        sql = make_insert_query('dataset_replicas', ['dataset_id', 'site_id', 'group_id', 'is_complete', 'is_partial', 'is_custodial'])

        template = '({dataset_id},{site_id},{group_id},{is_complete},{is_partial},{is_custodial})'
        mapping = lambda r: {'dataset_id': dataset_ids[r.dataset.name], 'site_id': site_ids[r.site.name], 'group_id': group_ids[r.group.name] if r.group else 0, 'is_complete': r.is_complete, 'is_partial': r.is_partial, 'is_custodial': r.is_custodial}

        self._query_many(sql, template, mapping, all_replicas)

        all_replicas = None # just to save some memory

        # insert/update blocks for this dataset
        sql = make_insert_query('blocks', ['name', 'dataset_id', 'size', 'num_files', 'is_open'])

        template = '(\'{name}\',{dataset_id},{size},{num_files},{is_open})'
        mapping = lambda b: {'name': b.name, 'dataset_id': dataset_ids[b.dataset.name], 'size': b.size, 'num_files': b.num_files, 'is_open': b.is_open}

        all_blocks = sum([d.blocks for d in datasets.values()], [])

        self._query_many(sql, template, mapping, all_blocks)

        all_blocks = None

        # insert/update block replicas for non-complete dataset replicas

        block_replicas = []

        for dataset in datasets.values():
            dataset_id = dataset_ids[dataset.name]

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
                block_ids = dict(self._query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id))

            for replica in dataset.replicas:
                site = replica.site
                site_id = site_ids[site.name]

                if replica not in need_blocklevel:
                    # this is a complete replica. Remove block replica for this dataset replica if required
                    if clean_stale:
                        self._delete_in('block_replicas', 'block_id', ('id', 'blocks', '`dataset_id` = %d' % dataset_id), '`site_id` = %d' % site_id)

                    continue

                # add the block replicas on this site to block_replicas together with SQL ID
                for block in dataset.blocks:
                    block_replicas += [(r, block_ids[block.name]) for r in block.replicas if r.site == site]

        sql = make_insert_query('block_replicas', ['block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial', 'time_created', 'time_updated'])

        template = '({block_id},{site_id},{group_id},{is_complete},{is_custodial},FROM_UNIXTIME({time_created}),FROM_UNIXTIME({time_updated}))'
        mapping = lambda (r, bid): {'block_id': bid, 'site_id': site_ids[r.site.name], 'group_id': group_ids[r.group.name] if r.group else 0, 'is_complete': r.is_complete, 'is_custodial': r.is_custodial, 'time_created': r.time_created, 'time_updated': r.time_updated}

        self._query_many(sql, template, mapping, block_replicas)

        # done inserting new / updated information

        if clean_stale:
            logger.info('Cleaning up stale data.')
    
            if len(sites) != 0:
                self._delete_not_in('sites', 'id', [site_ids[site_name] for site_name in sites])
    
            if len(groups) != 0:
                self._delete_not_in('groups', 'id', [group_ids[group_name] for group_name in groups])
    
            if len(datasets) != 0:
                self._delete_not_in('datasets', 'id', [dataset_ids[dataset_name] for dataset_name in datasets])
    
            self._delete_not_in('dataset_replicas', 'dataset_id', ('id', 'datasets'))
    
            self._delete_not_in('dataset_replicas', 'site_id', ('id', 'sites'))
    
            self._delete_not_in('blocks', 'dataset_id', ('id', 'datasets'))
    
            self._delete_not_in('block_replicas', 'block_id', ('id', 'blocks'))
    
            self._delete_not_in('block_replicas', 'site_id', ('id', 'sites'))

            self._delete_not_in('datasets', 'id', '(SELECT DISTINCT(`dataset_id`) FROM `dataset_replicas`)')

            self._delete_not_in('blocks', 'id', '(SELECT DISTINCT(`block_id`) FROM `block_replicas`)')

        # time stamp the inventory
        self._query('UPDATE `system` SET `last_update` = NOW()')
        self.last_update = self._query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

    def _do_delete_dataset(self, dataset): #override
        self._query('DELETE FROM `datasets` WHERE `name` LIKE %s', dataset.name)

    def _do_delete_block(self, block): #override
        self._query('DELETE FROM `blocks` WHERE `name` LIKE %s', block.name)

    def _do_delete_datasetreplicas(self, site, datasets, delete_blockreplicas): #override
        site_id = self._query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        sql = 'SELECT `id` FROM `datasets` WHERE `name` IN ({names})'
        names = ','.join(['\'%s\'' % dataset.name for d in datasets])
        dataset_ids = self._query(sql.format(names = names))
        dataset_ids_str = ','.join(map(str, dataset_ids))

        sql = 'DELETE FROM `dataset_replicas` WHERE `dataset_id` IN ({dataset_ids}) AND `site_id` = {site_id}'
        self._query(sql.format(dataset_ids = dataset_ids_str, site_id = site_id))

        if delete_blockreplicas:
            sql = 'DELETE FROM `block_replicas` WHERE `site_id` = {site_id} AND `block_id` IN (SELECT `id` FROM `blocks` WHERE `dataset_id` IN ({dataset_ids}))'.format(site_id = site_id, dataset_ids = dataset_ids_str)
            self._query(sql)

    def _do_delete_blockreplicas(self, replica_list): #override
        # Mass block replica deletion typically happens for a few sites and a few datasets.
        # Fetch site id first to avoid a long query.
        sites = list(set([r.site for r in replica_list])) # list of unique sites
        datasets = list(set([r.block.dataset for r in replica_list])) # list of unique sites
        
        site_names = ','.join(['\'%s\'' % s.name for s in sites])
        dataset_names = ','.join(['\'%s\'' % d.name for d in datasets])

        site_ids = {}
        sql = 'SELECT `name`, `id` FROM `sites` WHERE `name` IN ({names})'
        result = self._query(sql.format(names = site_names))
        for site_name, site_id in result:
            site = next(s for s in sites if s.name == site_name)
            site_ids[site] = site_id

        dataset_ids = {}
        sql = 'SELECT `name`, `id` FROM `datasets` WHERE `name` IN ({names})'
        result = self._query(sql.format(names = dataset_names))
        for dataset_name, dataset_id in result:
            dataset = next(d for d in datasets if d.name == dataset_name)
            dataset_ids[dataset] = dataset_id

        sql = 'DELETE FROM `block_replicas` AS replicas'
        sql += ' INNER JOIN `blocks` ON `blocks`.`id` = replicas.`block_id`'
        sql += ' WHERE (replicas.`site_id`, `blocks`.`dataset_id`, `blocks`.`name`) IN ({combinations})'

        combinations = ','.join(['(%d,%d,\'%s\')' % (site_ids[r.site], dataset_ids[r.block.dataset], r.block.name) for r in replica_list])

        self._query(sql.format(combinations = combinations))

    def _do_close_block(self, dataset_name, block_name): #override
        self._query('UPDATE `blocks` INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id` SET `blocks`.`is_open` = 0 WHERE `datasets`.`name` LIKE %s AND `blocks`.`name` LIKE %s', dataset_name, block_name)

    def _do_set_dataset_status(self, dataset_name, status_str): #override
        self._query('UPDATE `datasets` SET `status` = %s WHERE `name` LIKE %s', status_str, dataset_name)

    def _query(self, sql, *args):
        cursor = self.connection.cursor()

        logger.debug(sql)

        cursor.execute(sql, args)

        result = cursor.fetchall()

        if cursor.description is None:
            # insert query
            return cursor.lastrowid

        elif len(result) != 0 and len(result[0]) == 1:
            # single column requested
            return [row[0] for row in result]

        else:
            return list(result)

    def _query_many(self, sql, template, mapping, objects):
        cursor = self.connection.cursor()

        result = []

        values = ''
        for obj in objects:
            if values:
                values += ','

            replacements = mapping(obj)
            values += template.format(**replacements)
            
            # MySQL allows queries up to 1M characters
            if len(values) > config.mysql.max_query_len:
                logger.debug(sql % values)
                try:
                    cursor.execute(sql % values)
                except:
                    print sql % values
                result += cursor.fetchall()

                values = ''

        if values:
            logger.debug(sql % values)
            cursor.execute(sql % values)
            result += cursor.fetchall()

        return result

    def _delete_many(self, table, key, pool, delete_match, *conds):
        # need to repeat in case pool is a long list
        iP = 0
        while iP < len(pool):
            condition = '`%s`' % key
            if delete_match:
                condition += ' IN '
            else:
                condition += ' NOT IN '

            if type(pool) is tuple:
                if len(pool) == 2:
                    condition += '(SELECT `%s` FROM `%s`)' % pool
                elif len(pool) == 3:
                    condition += '(SELECT `%s` FROM `%s` WHERE %s)' % pool

                iP = len(pool) # will exit loop
    
            elif type(pool) is list:
                condition += '('

                query_len = len(condition)
                items = []
                while query_len < config.mysql.max_query_len and iP < len(pool):
                    item = str(pool[iP])
                    query_len += len(item)
                    items.append(item)
                    iP += 1

                condition += ','.join(items)
                condition += ')'
    
            conditions = [condition]
    
            for cond in conds:
                conditions.append(cond)
    
            sql = 'DELETE FROM `%s` WHERE ' % table
            sql += ' AND '.join(conditions)
            
            self._query(sql)

    def _delete_in(self, table, key, pool, *conds):
        self._delete_many(table, key, pool, True, *conds)

    def _delete_not_in(self, table, key, pool, *conds):
        self._delete_many(table, key, pool, False, *conds)
