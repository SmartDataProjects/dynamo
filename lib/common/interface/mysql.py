import os
import time
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

            logger.warning('Failed to database. Waiting 30 seconds..')

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

    def _do_load_data(self): #override

        # Load sites
        site_list = {}

        sites = self._query('SELECT `id`, `name`, `host`, `storage_type`, `backend`, `capacity`, `used_total` FROM `sites`')
        
        for site_id, name, host, storage_type, backend, capacity, used_total in sites:
            site = Site(name, host = host, storage_type = Site.storage_type(storage_type), backend = backend, capacity = capacity, used_total = used_total)

            site_list[name] = site

            self._site_ids[site] = site_id

        # Load groups
        group_list = {}

        groups = self._query('SELECT `id`, `name` FROM `groups`')

        for group_id, name in groups:
            group = Group(name)

            group_list[name] = group

        # Load datasets
        dataset_list = {}

        datasets = self._query('SELECT `id`, `name`, `size`, `num_files`, `is_open` FROM `datasets`')

        for dataset_id, name, size, num_files, is_open in datasets:
            dataset_list[name] = Dataset(name, size = size, num_files = num_files, is_open = is_open)

            self.dataset_ids[name] = dataset_id

        # Load blocks
        block_list = {}
            
        blocks = self._query('SELECT ds.`name`, bl.`id`, bl.`name`, bl.`size`, bl.`num_files`, bl.`is_open` FROM `blocks` AS bl INNER JOIN `datasets` AS ds ON ds.`id` = bl.`dataset_id`')

        for dsname, blid, name, size, num_files, is_open in blocks:
            block = Block(name, size = size, num_files = num_files, is_open = is_open)
            block.dataset = dataset_list[dsname]

            block_list[blid] = block

        # Link datasets to sites
        dataset_replicas = self._query('SELECT ds.`name`, st.`name`, rp.`is_partial`, rp.`is_custodial` FROM `dataset_replicas` AS rp INNER JOIN `datasets` AS ds ON ds.`id` = rp.`dataset_id` INNER JOIN `sites` AS st ON st.`id` = rp.`site_id`')

        for dsname, sitename, is_partial, is_custodial in dataset_replicas:
            dataset = dataset_list[dsname]
            site = site_list[sitename]

            rep = DatasetReplica(dataset, site, is_partial = is_partial, is_custodial = is_custodial)

            dataset.replicas.append(rep)
            site.datasets.append(dataset)

        # Link blocks to sites and groups
        block_replicas = self._query('SELECT bl.`id`, st.`name`, gr.`name`, rp.`is_custodial`, UNIX_TIMESTAMP(rp.`time_created`), UNIX_TIMESTAMP(rp.`time_updated`) FROM `block_replicas` AS rp INNER JOIN `blocks` AS bl ON bl.`id` = rp.`block_id` INNER JOIN `sites` AS st ON st.`id` = rp.`site_id` INNER JOIN `groups` AS gr ON gr.`id` = rp.`group_id`')

        for blid, sitename, groupname, is_custodial, time_created, time_updated in block_replicas:
            block = block_list[blid]
            site = site_list[sitename]
            group = group_list[groupname]

            rep = BlockReplica(block, site, group = group, is_custodial = is_custodial, time_created = time_created, time_updated = time_updated)

            block.replicas.append(rep)
            site.blocks.append(block)

        # Only the list of sites, groups, and datasets are returned
        return site_list, group_list, dataset_list

    def _do_save_data(self, site_list, group_list, dataset_list): #override

        def make_insert_query(table, fields):
            sql = 'INSERT INTO `' + table + '` (' + ','.join(['`{f}`'.format(f = f) for f in fields]) + ') VALUES %s'
            sql += ' ON DUPLICATE KEY UPDATE ' + ','.join(['`{f}`=VALUES(`{f}`)'.format(f = f) for f in fields])

            return sql

        # insert/update sites
        sql = make_insert_query('sites', ['name', 'host', 'storage_type', 'backend', 'capacity', 'used_total'])

        template = '(\'{name}\',\'{host}\',\'{storage_type}\',\'{backend}\',{capacity},{used_total})'
        mapping = lambda s: {'name': s.name, 'host': s.host, 'storage_type': Site.storage_type(s.storage_type), 'backend': s.backend, 'capacity': s.capacity, 'used_total': s.used_total}

        self._query_many(sql, template, mapping, site_list.values())

        # insert/update groups
        sql = make_insert_query('groups', ['name'])

        template = '(\'{name}\')'
        mapping = lambda g: {'name': g.name}

        self._query_many(sql, template, mapping, group_list.values())

        # insert/update datasets
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

            # insert/update dataset replicas
            sql = make_insert_query('dataset_replicas', ['dataset_id', 'site_id', 'is_partial', 'is_custodial'])

            template = '(%d,{site_id},{is_partial},{is_custodial})' % dataset_id
            mapping = lambda r: {'site_id': site_ids[r.site.name], 'is_partial': r.is_partial, 'is_custodial': r.is_custodial}

            self._query_many(sql, template, mapping, dataset.replicas)
            
            # deal with blocks only if dataset is partial on some site
            if len(filter(lambda r: r.is_partial, dataset.replicas)) != 0:
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

        self._query('UPDATE `system` SET `last_update` = NOW()')
        self.last_update = self._query('SELECT `last_update` FROM `system`')[0]

    def _do_clean_block_info(self): #override
        self._query('DELETE FROM `blocks` WHERE `id` NOT IN (SELECT DISTINCT(`block_id`) FROM `block_replicas`)')

    def _do_clean_dataset_info(self): #override
        self._query('DELETE FROM `datasets` WHERE `id` NOT IN (SELECT DISTINCT(`dataset_id`) FROM `dataset_replicas`)')

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

        values = ''
        for obj in objects:
            if values:
                values += ','

            replacements = mapping(obj)
            values += template.format(**replacements)
            
            if len(values) > 1024 * 512:
                cursor.execute(sql % values)
                values = ''

        logger.debug(sql % values)

        cursor.execute(sql % values)

        return cursor.fetchall()
