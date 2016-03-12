import os
import time
import re
import socket
import logging
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
            if table != 'system':
                self._query('INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`' % (new_db, table, db, table))

                if clear == InventoryInterface.CLEAR_ALL or \
                   (clear == InventoryInterface.CLEAR_REPLICAS and table in ['dataset_replicas', 'block_replicas']):
                    self._query('DROP TABLE `%s`.`%s`' % (db, table))
                    self._query('CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`' % (db, table, new_db, table))
       
        self._query('INSERT INTO `%s`.`system` (`lock_host`,`lock_process`) VALUES (\'\',0)' % new_db)

    def _do_remove_snapshot(self, older_than): #override
        databases = self._query('SHOW DATABASES')
        
        for database in databases:
            # find archived databases
            matches = re.match(self._db_params['db'] + '_([0-9]{12})', database)

            if not matches:
                continue

            timestamp = time.strptime(matches.group(1), '%y%m%d%H%M%S')
            if time.mktime(timestamp) < older_than:
                # this snapshot is old
                logger.info('Dropping database ' + database)
                self._query('DROP DATABASE ' + database)

    def _do_load_data(self): #override

        # Load sites
        site_list = []
        site_map = {} # id -> site

        sites = self._query('SELECT `id`, `name`, `host`, `storage_type`, `backend`, `capacity`, `used_total` FROM `sites`')

        logger.info('Loaded data for %d sites.', len(sites))
        
        for site_id, name, host, storage_type, backend, capacity, used_total in sites:
            site = Site(name, host = host, storage_type = Site.storage_type(storage_type), backend = backend, capacity = capacity, used_total = used_total)
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

        # Load datasets
        dataset_list = []
        dataset_map = {} # id -> site

        datasets = self._query('SELECT `id`, `name`, `size`, `num_files`, `is_open` FROM `datasets`')

        logger.info('Loaded data for %d datasets.', len(datasets))

        for dataset_id, name, size, num_files, is_open in datasets:
            dataset = Dataset(name, size = size, num_files = num_files, is_open = is_open)
            dataset_list.append(dataset)

            dataset_map[dataset_id] = dataset

        # Load blocks
        block_map = {} # id -> block
            
        blocks = self._query('SELECT `id`, `dataset_id`, `name`, `size`, `num_files`, `is_open` FROM `blocks`')

        logger.info('Loaded data for %d blocks.', len(blocks))

        for block_id, dataset_id, name, size, num_files, is_open in blocks:
            block = Block(name, size = size, num_files = num_files, is_open = is_open)

            dataset = dataset_map[ds_name]
            block.dataset = dataset
            dataset.blocks.append(block)

            block_map[block_id] = block

        logger.info('Linking datasets to sites.')

        # Link datasets to sites
        dataset_replicas = self._query('SELECT `dataset_id`, `site_id`, `is_partial`, `is_custodial` FROM `dataset_replicas`')

        for dataset_id, site_id, is_partial, is_custodial in dataset_replicas:
            dataset = dataset_map[dataset_id]
            site = site_map[site_id]

            rep = DatasetReplica(dataset, site, is_partial = is_partial, is_custodial = is_custodial)

            dataset.replicas.append(rep)
            site.datasets.append(dataset)

        logger.info('Linking blocks to sites.')

        # Link blocks to sites and groups
        block_replicas = self._query('SELECT `block_id`, `site_id`, `group_id`, `is_custodial`, UNIX_TIMESTAMP(`time_created`), UNIX_TIMESTAMP(`time_updated`) FROM `block_replicas`')

        for block_id, site_id, group_id, is_custodial, time_created, time_updated in block_replicas:
            block = block_map[block_id]
            site = site_map[site_id]
            group = group_map[group_id]

            rep = BlockReplica(block, site, group = group, is_custodial = is_custodial, time_created = time_created, time_updated = time_updated)

            block.replicas.append(rep)
            site.blocks.append(block)

        self.last_update = self._query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

        # Only the list of sites, groups, and datasets are returned
        return site_list, group_list, dataset_list

    def _do_save_data(self, site_list, group_list, dataset_list): #override

        def make_insert_query(table, fields):
            sql = 'INSERT INTO `{table}` ({fields}) VALUES %s'.format(table = table, fields = ','.join(['`%s`' % f for f in fields]))
            sql += ' ON DUPLICATE KEY UPDATE ' + ','.join(['`{f}`=VALUES(`{f}`)'.format(f = f) for f in fields])

            return sql

        def make_delete_query(table, key, pool):
            sql = 'DELETE FROM `{table}` WHERE `{key}` NOT IN '.format(table = table, key = key)
            if type(pool) is tuple:
                sql += '(SELECT `%s` FROM `%s`)' % pool
            else:
                sql += '(%s)' % (','.join(pool))
            
            return sql

        # insert/update sites
        logger.info('Inserting/updating %d sites.', len(site_list))

        sql = make_insert_query('sites', ['name', 'host', 'storage_type', 'backend', 'capacity', 'used_total'])

        template = '(\'{name}\',\'{host}\',\'{storage_type}\',\'{backend}\',{capacity},{used_total})'
        mapping = lambda s: {'name': s.name, 'host': s.host, 'storage_type': Site.storage_type(s.storage_type), 'backend': s.backend, 'capacity': s.capacity, 'used_total': s.used_total}

        self._query_many(sql, template, mapping, site_list.values())

        # insert/update groups
        logger.info('Inserting/updating %d groups.', len(group_list))

        sql = make_insert_query('groups', ['name'])

        template = '(\'{name}\')'
        mapping = lambda g: {'name': g.name}

        self._query_many(sql, template, mapping, group_list.values())

        # insert/update datasets
        logger.info('Inserting/updating %d datasets.', len(dataset_list))

        sql = make_insert_query('datasets', ['name', 'size', 'num_files', 'is_open'])

        template = '(\'{name}\',{size},{num_files},{is_open})'
        mapping = lambda d: {'name': d.name, 'size': d.size, 'num_files': d.num_files, 'is_open': d.is_open}

        self._query_many(sql, template, mapping, dataset_list.values())

        # make name -> id maps for use later
        site_ids = dict(self._query('SELECT `name`, `id` FROM `sites`'))
        group_ids = dict(self._query('SELECT `name`, `id` FROM `groups`'))
        dataset_ids = dict(self._query('SELECT `name`, `id` FROM `datasets`'))

        for ds_name, dataset in dataset_list.items():
            dataset_id = dataset_ids[ds_name]

            logger.info('Updating block and replica info for dataset %s.', ds_name)

            # insert/update dataset replicas
            sql = make_insert_query('dataset_replicas', ['dataset_id', 'site_id', 'is_partial', 'is_custodial'])

            template = '(%d,{site_id},{is_partial},{is_custodial})' % dataset_id
            mapping = lambda r: {'site_id': site_ids[r.site.name], 'is_partial': r.is_partial, 'is_custodial': r.is_custodial}

            self._query_many(sql, template, mapping, dataset.replicas)
            
            # deal with blocks only if dataset is partial on some site
            if len(filter(lambda r: r.is_partial, dataset.replicas)) == 0:
                continue

            # insert/update blocks for this dataset
            sql = make_insert_query('blocks', ['name', 'dataset_id', 'size', 'num_files', 'is_open'])

            template = '(\'{name}\',%d,{size},{num_files},{is_open})' % dataset_id
            mapping = lambda b: {'name': b.name, 'size': b.size, 'num_files': b.num_files, 'is_open': b.is_open}

            self._query_many(sql, template, mapping, dataset.blocks)

            block_ids = dict(self._query('SELECT `name`, `id` FROM `blocks` WHERE `dataset_id` = %s', dataset_id))

            for block in dataset.blocks:
                block_id = block_ids[block.name]

                # insert/update block replicas
                sql = make_insert_query('block_replicas', ['block_id', 'site_id', 'group_id', 'is_custodial', 'time_created', 'time_updated'])

                template = '(%d,{site_id},{group_id},{is_custodial},FROM_UNIXTIME({time_created}),FROM_UNIXTIME({time_updated}))' % block_id
                mapping = lambda r: {'site_id': site_ids[r.site.name], 'group_id': group_ids[r.group.name] if r.group else 0, 'is_custodial': r.is_custodial, 'time_created': r.time_created, 'time_updated': r.time_updated}
    
                self._query_many(sql, template, mapping, block.replicas)

        # delete items not current
        logger.info('Cleaning up stale data.')

        sql = make_delete_query('sites', 'id', [str(site_ids[name]) for name in site_list])
        self._query(sql)

        sql = make_delete_query('groups', 'id', [str(group_ids[name]) for name in group_list])
        self._query(sql)

        sql = make_delete_query('datasets', 'id', [str(dataset_ids[name]) for name in dataset_list])
        self._query(sql)

        sql = make_delete_query('dataset_replicas', 'dataset_id', ('id', 'datasets'))
        self._query(sql)

        sql = make_delete_query('dataset_replicas', 'site_id', ('id', 'sites'))
        self._query(sql)

        sql = make_delete_query('blocks', 'dataset_id', ('id', 'datasets'))
        self._query(sql)

        sql = make_delete_query('block_replicas', 'block_id', ('id', 'blocks'))
        self._query(sql)

        sql = make_delete_query('block_replicas', 'site_id', ('id', 'sites'))
        self._query(sql)

        # time stamp the inventory
        self._query('UPDATE `system` SET `last_update` = NOW()')
        self.last_update = self._query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

    def _do_clean_block_info(self): #override
        self._query('DELETE FROM `blocks` WHERE `id` NOT IN (SELECT DISTINCT(`block_id`) FROM `block_replicas`)')

    def _do_clean_dataset_info(self): #override
        self._query('DELETE FROM `datasets` WHERE `id` NOT IN (SELECT DISTINCT(`dataset_id`) FROM `dataset_replicas`)')

    def _do_delete_dataset(self, dataset): #override
        self._query('DELETE FROM `datasets` WHERE `name` LIKE %s', dataset.name)

    def _do_delete_block(self, block): #override
        self._query('DELETE FROM `blocks` WHERE `name` LIKE %s', block.name)

    def _do_delete_datasetreplica(self, replica): #override
        self._query('DELETE FROM `dataset_replicas` WHERE `dataset_id` IN (SELECT `id` FROM `datasets` WHERE `name` LIKE %s) AND `site_id` IN (SELECT `id` FROM `sites` WHERE `name` LIKE %s)', replica.dataset.name, replica.site.name)

    def _do_delete_blockreplica(self, replica): #override
        self._query('DELETE FROM `block_replicas` WHERE `block_id` IN (SELECT `id` FROM `blocks` WHERE `name` LIKE %s) AND `site_id` IN (SELECT `id` FROM `sites` WHERE `name` LIKE %s)', replica.block.name, replica.site.name)

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
            
            if len(values) > 1024 * 512:
                logger.debug(sql % values)
                cursor.execute(sql % values)
                result += cursor.fetchall()

                values = ''

        if values:
            logger.debug(sql % values)
            cursor.execute(sql % values)
            result += cursor.fetchall()

        return result
