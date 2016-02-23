import os
import time
import socket
import MySQLdb

from common.interface.base import InventoryInterface
from common.dataformat import Dataset, Block, Site, DatasetReplica, BlockReplica

class MySQLInterface(InventoryInterface):
    """Interface to MySQL."""

    class DatabaseError(Exception):
        pass

    def __init__(self, **db_params):
        super(MySQLInterface, self).__init__()
        self.connection = MySQLdb.connect(**db_params)
        
        self.dataset_ids = {}
        self.block_ids = {}
        self.site_ids = {}

    def _do_acquire_lock(self): #override
        cursor = self.connection.cursor()

        while True:
            # Single MySQL query (atomic) to "software-lock" the database
            cursor.execute('UPDATE `system` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', (socket.hostname(), os.getpid()))
            # Did the update go through?
            cursor.execute('SELECT `lock_host`, `lock_process` FROM `system`')
            host, pid = cursor.fetchall()[0]

            if host == socket.hostname() and pid == os.getpid():
                # The database is locked.
                break

            time.sleep(30)

    def _do_release_lock(self): #override
        cursor = self.connection.cursor()

        cursor.execute('UPDATE `system` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', (socket.hostname(), os.getpid()))
        # Did the update go through?
        cursor.execute('SELECT `lock_host`, `lock_process` FROM `system_data`')
        host, pid = cursor.fetchall()[0]

        if host != '' or pid != 0:
            raise InventoryInterface.LockError('Failed to release lock from ' + socket.hostname() + ':' + str(os.getpid()))

    def _do_make_snapshot(self): #override
        # To be implemented
        pass

    def _do_prepare_new(self): #override
        cursor = self.connection.cursor()

        with open(os.path.dirname(os.path.realpath(__file__)) + '/mysql_prepare_new.sql') as queries:
            query = ''
            for line in queries:
                line = line.strip()
                if line == '':
                    continue

                if line.endswith(';'):
                    query += line[:-1]
                    cursor.execute(query)
                    query = ''

                else:
                    query += line

    def _get_known_dataset_names(self): #override
        cursor = self.connection.cursor()

        cursor.execute('SELECT `id`, `name` FROM `datasets`')
        names = []
        for dataset_id, name in cursor:
            names.append(name)
            self.dataset_ids[name] = dataset_id

        return names

    def _get_known_block_names(self): #override
        cursor = self.connection.cursor()

        cursor.execute('SELECT `id`, `name` FROM `blocks`')
        names = []
        for block_id, name in cursor:
            names.append(name)
            self.block_ids[name] = block_id

        return names

    def _get_known_site_names(self): #override
        cursor = self.connection.cursor()

        cursor.execute('SELECT `id`, `name` FROM `sites`')
        names = []
        for site_id, name in cursor:
            names.append(name)
            self.site_ids[name] = site_id

        return names

    def _do_create_dataset_info(self, dataset): #override
        cursor = self.connection.cursor()

        cursor.execute('INSERT INTO `datasets` (`name`, `size`, `num_files`, `is_open`) VALUES (%s, %s, %s, %s)', (dataset.name, dataset.size, dataset.num_files, dataset.is_open))
        self.dataset_ids[dataset.name] = cursor.lastrowid

    def _do_update_dataset_info(self, dataset): #override
        cursor = self.connection.cursor()

        cursor.execute('UPDATE `datasets` SET `size` = %s, `num_files` = %s, `is_open` = %s WHERE `id` = %s', (dataset.size, dataset.num_files, dataset.is_open, self.dataset_ids[dataset.name]))

    def _do_delete_dataset_info(self, name): #override
        cursor = self.connection.cursor()

        cursor.execute('DELETE FROM `datasets` WHERE `id` = %s', self.dataset_ids[name])

    def _do_create_dataset_info_list(self, datasets): #override
        sql = 'INSERT INTO `datasets` (`name`, `size`, `num_files`, `is_open`) VALUES %s'
        sql += ' ON DUPLICATE KEY UPDATE `size` = VALUES(`size`), `num_files` = VALUES(`num_files`), `is_open` = VALUES(`is_open`)'
        template = '(\'{name}\',{size},{num_files},{is_open})'
        mapping = lambda d: {'name': d.name, 'size': d.size, 'num_files': d.num_files, 'is_open': 0 if d.is_open else 1}
        self._query_many(sql, template, mapping, datasets)

        for name, dataset_id in self._find_ids('datasets', datasets):
            self.dataset_ids[name] = dataset_id

    def _do_update_dataset_info_list(self, datasets): #override
	self._do_create_dataset_info_list(datasets)

    def _do_delete_dataset_info_list(self, names): #override
        sql = 'DELETE FROM `datasets` WHERE `id` IN (%s)'
        template = '{id}'
        mapping = lambda n: {'id': self.dataset_ids[n]}
        self._query_many(sql, template, mapping, names)

    def _do_create_block_info(self, block): #override
        cursor = self.connection.cursor()

        cursor.execute('INSERT INTO `blocks` (`name`, `size`, `num_files`, `is_open`) VALUES (%s, %s, %s, %s)', (block.name, block.size, block.num_files, block.is_open))
        self.block_ids[block.name] = cursor.lastrowid

    def _do_update_block_info(self, block): #override
        cursor = self.connection.cursor()

        cursor.execute('UPDATE `blocks` SET `size` = %s, `num_files` = %s, `is_open` = %s WHERE `id` = %s', (block.size, block.num_files, block.is_open, self.block_ids[block.name]))

    def _do_delete_block_info(self, name): #override
        cursor = self.connection.cursor()

        cursor.execute('DELETE FROM `blocks` WHERE `id` = %s', self.block_ids[name])

    def _do_create_block_info_list(self, blocks): #override
        sql = 'INSERT INTO `blocks` (`name`, `dataset_id`, `size`, `num_files`, `is_open`) VALUES %s'
        sql += ' ON DUPLICATE KEY UPDATE `dataset_id` = VALUES(`dataset_id`), `size` = VALUES(`size`), `num_files` = VALUES(`num_files`), `is_open` = VALUES(`is_open`)'
        template = '(\'{name}\',{dataset_id},{size},{num_files},{is_open})'
        mapping = lambda b: {'name': b.name, 'dataset_id': self.dataset_ids[b.dataset.name], 'size': b.size, 'num_files': b.num_files, 'is_open': 0 if b.is_open else 1}

        self._query_many(sql, template, mapping, blocks)

        for name, block_id in self._find_ids('blocks', blocks):
            self.block_ids[name] = block_id

    def _do_update_block_info_list(self, blocks): #override
        self._do_create_block_info_list(blocks)

    def _do_delete_block_info_list(self, names): #override
        sql = 'DELETE FROM `blocks` WHERE `id` IN (%s)'
        template = '{id}'
        mapping = lambda n: {'id': self.block_ids[n]}

        self._query_many(sql, template, mapping, names)

    def _do_create_site_info(self, site): #override
        cursor = self.connection.cursor()

        cursor.execute('INSERT INTO `sites` (`name`, `capacity`, `used_total`) VALUES (%s, %s, %s)', (site.name, site.capacity, site.used_total))
        self.site_ids[site.name] = cursor.lastrowid

    def _do_update_site_info(self, site): #override
        cursor = self.connection.cursor()

        cursor.execute('UPDATE `sites` SET `capacity` = %s, `used_total` = %s WHERE `id` = %s', (site.capacity, site.used_total, self.site_ids[site.name]))

    def _do_delete_site_info(self, name): #override
        cursor = self.connection.cursor()

        cursor.execute('DELETE FROM `sites` WHERE `id` = %s', self.site_ids[name])

    def _do_create_site_info_list(self, sites): #override
        sql = 'INSERT INTO `sites` (`name`, `capacity`, `used_total`) VALUES %s'
        sql += ' ON DUPLICATE KEY UPDATE `capacity` = VALUES(`capacity`), `used_total` = VALUES(`used_total`)'
        template = '(\'{name}\',{capacity},{used_total})'
        mapping = lambda s: {'name': s.name, 'capacity': s.capacity, 'used_total': s.used_total}

        self._query_many(sql, template, mapping, sites)

        for name, site_id in self._find_ids('sites', sites):
            self.site_ids[name] = site_id

    def _do_update_site_info_list(self, sites): #override
        self._do_create_site_info_list(sites)

    def _do_delete_site_info_list(self, names): #override
        self._delete_all('sites', names)

    def _do_place_dataset(self, dataset_replicas): #override
        sql = 'INSERT INTO `dataset_replicas` (`dataset_id`, `site_id`, `is_partial`) VALUES %s'
        template = '({dataset_id},{site_id},{is_partial})'
        mapping = lambda r: {'dataset_id': self.dataset_ids[r.dataset.name], 'site_id': self.site_ids[r.site.name], 'is_partial': 1 if r.is_partial else 0}

        self._query_many(sql, template, mapping, dataset_replicas)

    def _do_place_block(self, block_replicas): #override
        sql = 'INSERT INTO `block_replicas` (`block_id`, `site_id`) VALUES %s'
        sql += ' ON DUPLICATE KEY UPDATE `block_id` = VALUES(`block_id`)' # dummy operation to ignore duplicates

        template = '({block_id},{site_id})'
        mapping = lambda r: {'block_id': self.block_ids[r.block.name], 'site_id': self.site_ids[r.site.name]}

        self._query_many(sql, template, mapping, block_replicas)

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

        cursor.execute(sql % values)

        return cursor.fetchall()

    def _delete_all(self, table, names):
        sql = 'DELETE FROM `' + table + '` WHERE `name` IN (%s)'
        template = '\'{name}\''
        mapping = lambda n: {'name': n}

        self._query_many(sql, template, mapping, names)

    def _find_ids(self, table, objs):
        sql = 'SELECT `name`, `id` FROM `' + table + '` WHERE `name` IN (%s)'
        template = '\'{name}\''
        mapping = lambda o: {'name': o.name}

        return self._query_many(sql, template, mapping, objs)
