import os
import socket
import logging
import time
import re
import collections
import sqlite3
import lzma

from dynamo.history.history import TransactionHistoryInterface
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import HistoryRecord

LOG = logging.getLogger(__name__)

class MySQLHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self, config):
        TransactionHistoryInterface.__init__(self, config)

        self._mysql = MySQL(config.db_params)
        self._cache_db = MySQL(config.cache_db_params)

        self._site_id_map = {}
        self._dataset_id_map = {}

    def _do_acquire_lock(self, blocking): #override
        while True:
            # Use the system table to "software-lock" the database
            self._mysql.query('LOCK TABLES `lock` WRITE')
            self._mysql.query('UPDATE `lock` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', socket.gethostname(), os.getpid())

            # Did the update go through?
            host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
            self._mysql.query('UNLOCK TABLES')

            if host == socket.gethostname() and pid == os.getpid():
                # The database is locked.
                break
            
            if blocking:
                LOG.warning('Failed to lock database. Waiting 30 seconds..')
                time.sleep(30)
            else:
                LOG.warning('Failed to lock database.')
                return False

        return True

    def _do_release_lock(self, force): #override
        self._mysql.query('LOCK TABLES `lock` WRITE')
        if force:
            self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0')
        else:
            self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise RuntimeError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_new_run(self, operation, partition, policy_version, comment): #override
        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            part_id = self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
        else:
            part_id = part_ids[0]

        if operation == HistoryRecord.OP_COPY:
            if self.config.get('test', False):
                operation_str = 'copy_test'
            else:
                operation_str = 'copy'
        else:
            if self.config.get('test', False):
                operation_str = 'deletion_test'
            else:
                operation_str = 'deletion'

        return self._mysql.query('INSERT INTO `runs` (`operation`, `partition_id`, `policy_version`, `comment`, `time_start`) VALUES (%s, %s, %s, %s, NOW())', operation_str, part_id, policy_version, comment)

    def _do_close_run(self, operation, run_number): #override
        self._mysql.query('UPDATE `runs` SET `time_end` = FROM_UNIXTIME(%s) WHERE `id` = %s', time.time(), run_number)

    def _do_make_copy_entry(self, run_number, site, operation_id, approved, dataset_list, size): #override
        """
        Site and datasets are expected to be already in the database.
        """

        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        self._mysql.query('INSERT INTO `copy_requests` (`id`, `run_id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', operation_id, run_number, approved, self._site_id_map[site.name], size)

        self._mysql.insert_many('copied_replicas', ('copy_id', 'dataset_id'), lambda d: (operation_id, self._dataset_id_map[d.name]), dataset_list)

    def _do_make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size): #override
        """
        site and dataset are expected to be already in the database (save_deletion_decisions should be called first).
        """

        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = self._mysql.select_many('datasets', ('id',), 'name', (d.name for d in datasets))

        self._mysql.query('INSERT INTO `deletion_requests` (`id`, `run_id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', operation_id, run_number, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda did: (operation_id, did), dataset_ids)

    def _do_update_copy_entry(self, copy_record): #override
        self._mysql.query('UPDATE `copy_requests` SET `approved` = %s, `size` = %s, `completed` = %s WHERE `id` = %s', copy_record.approved, copy_record.size, copy_record.completed, copy_record.operation_id)
        
    def _do_update_deletion_entry(self, deletion_record): #override
        self._mysql.query('UPDATE `deletion_requests` SET `approved` = %s, `size` = %s WHERE `id` = %s', deletion_record.approved, deletion_record.size, deletion_record.operation_id)

    def _do_save_sites(self, sites): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        names_to_sites = dict((s.name, s) for s in sites)

        sites_to_insert = []
        for site_name in names_to_sites.iterkeys():
            if site_name not in self._site_id_map:
                sites_to_insert.append(site_name)

        if len(sites_to_insert) != 0:
            self._mysql.insert_many('sites', ('name',), None, sites_to_insert)
            self._make_site_id_map()

    def _do_get_sites(self, run_number): #override
        self._fill_site_snapshot_cache(run_number)

        table_name = 'sites_%d' % run_number

        sql = 'SELECT s.`name`, n.`status`, n.`quota` FROM `%s`.`%s` AS n' % (self._cache_db.db_name(), table_name)
        sql += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = n.`site_id`' % self._mysql.db_name()

        sites_dict = {}

        for site_name, status, quota in self._mysql.xquery(sql):
            sites_dict[site_name] = (status, quota)

        return sites_dict

    def _do_save_datasets(self, datasets): #override
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        datasets_to_insert = set(d.name for d in datasets) - set(self._dataset_id_map.iterkeys())
        if len(datasets_to_insert) == 0:
            return

        self._mysql.insert_many('datasets', ('name',), None, datasets_to_insert)
        self._make_dataset_id_map()

    def _do_save_conditions(self, policy_lines): #ovrride
        for line in policy_lines:
            text = re.sub('\s+', ' ', line.condition.text)
            ids = self._mysql.query('SELECT `id` FROM `policy_conditions` WHERE `text` LIKE %s', text)
            if len(ids) == 0:
                line.condition_id = self._mysql.query('INSERT INTO `policy_conditions` (`text`) VALUES (%s)', text)
            else:
                line.condition_id = ids[0]

    def _do_save_copy_decisions(self, run_number, copies): #override
        pass

    def _do_save_deletion_decisions(self, run_number, deleted_list, kept_list, protected_list): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()
            
        srun = '%09d' % run_number
        spool_dir_name = '%s/detox_snapshots' % (config.paths.spool)
        db_file_name = '%s/snapshot_%09d.db' % (spool_dir_name, run_number)

        try:
            os.makedirs(spool_dir_name)
            os.chmod(spool_dir_name, 0777)
        except OSError:
            pass

        if os.path.exists(db_file_name):
            os.unlink(db_file_name)

        LOG.info('Creating snapshot SQLite3 DB %s', db_file_name)

        # hardcoded!!
        replica_delete = 1
        replica_keep = 2
        replica_protect = 3

        snapshot_db = sqlite3.connect(db_file_name)
        snapshot_cursor = snapshot_db.cursor()

        sql = 'CREATE TABLE `decisions` ('
        sql += '`id` TINYINT PRIMARY KEY NOT NULL,'
        sql += '`value` TEXT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)
        snapshot_db.execute('INSERT INTO `decisions` VALUES (%d, \'delete\')' % replica_delete)
        snapshot_db.execute('INSERT INTO `decisions` VALUES (%d, \'keep\')' % replica_keep)
        snapshot_db.execute('INSERT INTO `decisions` VALUES (%d, \'protect\')' % replica_protect)

        sql = 'CREATE TABLE `replicas` ('
        sql += '`site_id` SMALLINT NOT NULL,'
        sql += '`dataset_id` INT NOT NULL,'
        sql += '`size` BIGINT NOT NULL,'
        sql += '`decision_id` TINYINT NOT NULL REFERENCES `decisions`(`id`),'
        sql += '`condition` MEDIUMINT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)
        snapshot_db.execute('CREATE INDEX `site_dataset` ON `replicas` (`site_id`, `dataset_id`)')

        sql = 'INSERT INTO `replicas` VALUES (?, ?, ?, ?, ?)'

        def do_insert(entries, decision):
            for replica, matches in entries.iteritems():
                site_id = self._site_id_map[replica.site.name]
                dataset_id = self._dataset_id_map[replica.dataset.name]

                for match in matches:
                    condition_id = match[1]
                    size = sum(r.size for r in match[0])

                    snapshot_cursor.execute(sql, (site_id, dataset_id, size, decision, condition_id))

            snapshot_db.commit()
        
        do_insert(deleted_list, replica_delete)
        do_insert(kept_list, replica_keep)
        do_insert(protected_list, replica_protect)
        
        snapshot_cursor.close()
        snapshot_db.close()

        os.chmod(db_file_name, 0666)

        self._fill_replica_snapshot_cache(run_number)

    def _do_save_quotas(self, run_number, quotas): #override
        # Will save quotas and statuses

        if len(self._site_id_map) == 0:
            self._make_site_id_map()
            
        srun = '%09d' % run_number
        db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_spool_dir, run_number)

        # hardcoded!!
        site_ready = 1
        site_waitroom = 2
        site_morgue = 3
        site_unknown = 4

        # DB file should exist already - this function is called after save_deletion_decisions

        snapshot_db = sqlite3.connect(db_file_name)
        snapshot_cursor = snapshot_db.cursor()

        sql = 'CREATE TABLE `statuses` ('
        sql += '`id` TINYINT PRIMARY KEY NOT NULL,'
        sql += '`value` TEXT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'ready\')' % site_ready)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'waitroom\')' % site_waitroom)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'morgue\')' % site_morgue)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'unknown\')' % site_unknown)

        sql = 'CREATE TABLE `sites` ('
        sql += '`site_id` SMALLINT PRIMARY KEY NOT NULL,'
        sql += '`status_id` TINYINT NOT NULL REFERENCES `statuses`(`id`),'
        sql += '`quota` INT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)

        sql = 'INSERT INTO `sites` VALUES (?, ?, ?)'

        for site, quota in quotas.iteritems():
            snapshot_cursor.execute(sql, (self._site_id_map[site.name], site.status, quota))

        snapshot_db.commit()

        snapshot_cursor.close()
        snapshot_db.close()

        self._fill_site_snapshot_cache(run_number)

        # Archive the sqlite3 file
        # Relying on the fact save_quotas is called after save_deletion_decisions

        archive_dir_name = '%s/detox_snapshots/%s/%s' % (config.paths.archive, srun[:3], srun[3:6])
        xz_file_name = '%s/snapshot_%09d.db.xz' % (archive_dir_name, run_number)

        try:
            os.makedirs(archive_dir_name)
        except OSError:
            pass

        with open(db_file_name, 'rb') as db_file:
            with open(xz_file_name, 'wb') as xz_file:
                xz_file.write(lzma.compress(db_file.read()))

    def _do_get_deletion_decisions(self, run_number, size_only): #override
        self._fill_replica_snapshot_cache(run_number)

        table_name = 'replicas_%d' % run_number

        if size_only:
            # return {site_name: (protect_size, delete_size, keep_size)}
            volumes = {}
            sites = set()

            query = 'SELECT s.`name`, SUM(r.`size`) * 1.e-12 FROM `%s`.`%s` AS r' % (self._cache_db.db_name(), table_name)
            query += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = r.`site_id`' % self._mysql.db_name()
            query += ' WHERE r.`decision` LIKE %s'
            query += ' GROUP BY r.`site_id`'

            for decision in ['protect', 'delete', 'keep']:
                volumes[decision] = dict(self._mysql.xquery(query, decision))
                sites.update(set(volumes[decision].iterkeys()))
               
            product = {}
            for site_name in sites:
                v = {}
                for decision in ['protect', 'delete', 'keep']:
                    try:
                        v[decision] = volumes[decision][site_name]
                    except:
                        v[decision] = 0

                product[site_name] = (v['protect'], v['delete'], v['keep'])

            return product

        else:
            # return {site_name: [(dataset_name, size, decision, reason)]}

            query = 'SELECT s.`name`, d.`name`, r.`size`, r.`decision`, p.`text` FROM `%s`.`%s` AS r' % (self._cache_db.db_name(), table_name)
            query += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = r.`site_id`' % self._mysql.db_name()
            query += ' INNER JOIN `%s`.`datasets` AS d ON d.`id` = r.`dataset_id`' % self._mysql.db_name()
            query += ' INNER JOIN `%s`.`policy_conditions` AS p ON p.`id` = r.`condition`' % self._mysql.db_name()
            query += ' ORDER BY s.`name` ASC, r.`size` DESC'

            product = {}

            _site_name = ''

            for site_name, dataset_name, size, decision, reason in self._cache_db.xquery(query):
                if site_name != _site_name:
                    product[site_name] = []
                    current = product[site_name]
                    _site_name = site_name
                
                current.append((dataset_name, size, decision, reason))

            return product

    def _do_save_dataset_popularity(self, run_number, datasets): #override
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        fields = ('run_id', 'dataset_id', 'popularity')
        mapping = lambda dataset: (run_number, self._dataset_id_map[dataset.name], dataset.attr['request_weight'] if 'request_weight' in dataset.attr else 0.)
        self._mysql.insert_many('dataset_popularity_snapshots', fields, mapping, datasets)

    def _do_get_incomplete_copies(self, partition): #override
        query = 'SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`'
        query += ' FROM `copy_requests` AS h'
        query += ' INNER JOIN `runs` AS r ON r.`id` = h.`run_id`'
        query += ' INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id`'
        query += ' INNER JOIN `sites` AS s ON s.`id` = h.`site_id`'
        query += ' WHERE h.`id` > 0 AND p.`name` LIKE \'%s\' AND h.`completed` = 0 AND h.`run_id` > 0' % partition
        history_entries = self._mysql.xquery(query)
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_COPY, eid, site_name, timestamp = timestamp, approved = approved, size = size)

        id_to_dataset = dict(self._mysql.xquery('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.xquery('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('copied_replicas', ('copy_id', 'dataset_id'), 'copy_id', id_to_record.iterkeys())

        current_copy_id = 0
        for copy_id, dataset_id in replicas:
            if copy_id != current_copy_id:
                record = id_to_record[copy_id]
                current_copy_id = copy_id

            record.replicas.append(HistoryRecord.CopiedReplica(dataset_name = id_to_dataset[dataset_id]))

        return id_to_record.values()

    def _do_get_copied_replicas(self, run_number): #override
        query = 'SELECT s.`name`, d.`name` FROM `copied_replicas` AS p'
        query += ' INNER JOIN `copy_requests` AS r ON r.`id` = p.`copy_id`'
        query += ' INNER JOIN `datasets` AS d ON d.`id` = p.`dataset_id`'
        query += ' INNER JOIN `sites` AS s ON s.`id` = r.`site_id`'
        query += ' WHERE r.`run_id` = %d' % run_number
        
        return self._mysql.query(query)

    def _do_get_site_name(self, operation_id): #override
        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `copy_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `deletion_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        return ''

    def _do_get_deletion_runs(self, partition, first, last): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return []

        partition_id = result[0]

        sql = 'SELECT `id` FROM `runs` WHERE `partition_id` = %d AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'deletion\', \'deletion_test\')' % partition_id

        if first >= 0:
            sql += ' AND `id` >= %d' % first
        if last >= 0:
            sql += ' AND `id` <= %d' % last

        sql += ' ORDER BY `id` ASC'

        result = self._mysql.query(sql)

        if first < 0 and len(result) > 1:
            result = result[-1:]

        return result

    def _do_get_copy_runs(self, partition, first, last): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return []

        partition_id = result[0]

        sql = 'SELECT `id` FROM `runs` WHERE `partition_id` = %d AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'copy\', \'copy_test\')' % partition_id

        if first >= 0:
            sql += ' AND `id` >= %d' % first
        if last >= 0:
            sql += ' AND `id` <= %d' % last

        sql += ' ORDER BY `id` ASC'

        if first < 0 and len(result) > 1:
            result = result[-1:]

        return result

    def _do_get_run_timestamp(self, run_number): #override
        result = self._mysql.query('SELECT UNIX_TIMESTAMP(`time_start`) FROM `runs` WHERE `id` = %s', run_number)
        if len(result) == 0:
            return 0

        return result[0]

    def _do_get_next_test_id(self): #override
        copy_result = self._mysql.query('SELECT MIN(`id`) FROM `copy_requests`')[0]
        if copy_result == None:
            copy_result = 0

        deletion_result = self._mysql.query('SELECT MIN(`id`) FROM `deletion_requests`')[0]
        if deletion_result == None:
            deletion_result = 0

        return min(copy_result, deletion_result) - 1

    def _make_site_id_map(self):
        self._site_id_map = {}
        for name, site_id in self._mysql.xquery('SELECT `name`, `id` FROM `sites`'):
            self._site_id_map[name] = int(site_id)

    def _make_dataset_id_map(self):
        self._dataset_id_map = {}
        for name, dataset_id in self._mysql.xquery('SELECT `name`, `id` FROM `datasets`'):
            self._dataset_id_map[name] = int(dataset_id)

    def _fill_replica_snapshot_cache(self, run_number):
        table_name = 'replicas_%d' % run_number
        sql = 'SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = %s AND `TABLE_NAME` = %s'
        if self._mysql.query(sql, self._cache_db.db_name(), table_name)[0] == 0:
            # cache table does not exist; fill from sqlite

            srun = '%09d' % run_number

            db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_archive_dir, run_number)
            if not os.path.exists(db_file_name):
                xz_file_name = '%s/%s/%s/snapshot_%09d.db.xz' % (self.config.snapshots_archive_dir, srun[:3], srun[3:6], run_number)
                if not os.path.exists(xz_file_name):
                    raise RuntimeError('Snapshot DB ' + db_file_name + ' does not exist')

                with open(xz_file_name, 'rb') as xz_file:
                    with open(db_file_name, 'wb') as db_file:
                        db_file.write(lzma.decompress(xz_file.read()))

            snapshot_db = sqlite3.connect(db_file_name)
            snapshot_db.text_factory = str # otherwise we'll get unicode and MySQLdb cannot convert that
            snapshot_cursor = snapshot_db.cursor()

            def make_snapshot_reader():
                sql = 'SELECT r.`site_id`, r.`dataset_id`, r.`size`, d.`value`, r.`condition` FROM `replicas` AS r'
                sql += ' INNER JOIN `decisions` AS d ON d.`id` = r.`decision_id`'
                snapshot_cursor.execute(sql)
                
                while True:
                    row = snapshot_cursor.fetchone()
                    if row is None:
                        return

                    yield row

            snapshot_reader = make_snapshot_reader()

            self._cache_db.query('CREATE TABLE `%s` LIKE `replicas`' % table_name)

            self._cache_db.insert_many(table_name, ('site_id', 'dataset_id', 'size', 'decision', 'condition'), None, snapshot_reader, do_update = False)

            snapshot_cursor.close()
            snapshot_db.close()

        self._cache_db.query('INSERT INTO `replica_snapshot_usage` VALUES (%s, NOW())', run_number)

        self._clean_old_cache()

    def _fill_site_snapshot_cache(self, run_number):
        table_name = 'sites_%d' % run_number
        sql = 'SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = %s AND `TABLE_NAME` = %s'
        if self._mysql.query(sql, self._cache_db.db_name(), table_name)[0] == 0:
            # cache table does not exist; fill from sqlite

            srun = '%09d' % run_number

            db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_spool_dir, run_number)
            if not os.path.exists(db_file_name):
                xz_file_name = '%s/%s/%s/snapshot_%09d.db.xz' % (self.config.snapshots_archive_dir, srun[:3], srun[3:6], run_number)
                if not os.path.exists(xz_file_name):
                    raise RuntimeError('Snapshot DB ' + db_file_name + ' does not exist')

                with open(xz_file_name, 'rb') as xz_file:
                    with open(db_file_name, 'wb') as db_file:
                        db_file.write(lzma.decompress(xz_file.read()))

            snapshot_db = sqlite3.connect(db_file_name)
            snapshot_db.text_factory = str # otherwise we'll get unicode and MySQLdb cannot convert that
            snapshot_cursor = snapshot_db.cursor()

            def make_snapshot_reader():
                sql = 'SELECT s.`site_id`, t.`value`, s.`quota` FROM `sites` AS s'
                sql += ' INNER JOIN `statuses` AS t ON t.`id` = s.`status_id`'
                snapshot_cursor.execute(sql)
                
                while True:
                    row = snapshot_cursor.fetchone()
                    if row is None:
                        return

                    yield row

            snapshot_reader = make_snapshot_reader()

            self._cache_db.query('CREATE TABLE `%s` LIKE `sites`' % table_name)

            self._cache_db.insert_many(table_name, ('site_id', 'status', 'quota'), None, snapshot_reader, do_update = False)

            snapshot_cursor.close()
            snapshot_db.close()

        self._cache_db.query('INSERT INTO `site_snapshot_usage` VALUES (%s, NOW())', run_number)

        self._clean_old_cache()

    def _clean_old_cache(self):
        sql = 'SELECT `run_id` FROM (SELECT `run_id`, MAX(`timestamp`) AS m FROM `replica_snapshot_usage` GROUP BY `run_id`) AS t WHERE m < DATE_SUB(NOW(), INTERVAL 1 WEEK)'
        old_replica_runs = self._cache_db.query(sql)
        for old_run in old_replica_runs:
            table_name = 'replicas_%d' % old_run
            self._cache_db.query('DROP TABLE IF EXISTS `%s`' % table_name)

        sql = 'SELECT `run_id` FROM (SELECT `run_id`, MAX(`timestamp`) AS m FROM `site_snapshot_usage` GROUP BY `run_id`) AS t WHERE m < DATE_SUB(NOW(), INTERVAL 1 WEEK)'
        old_site_runs = self._cache_db.query(sql)
        for old_run in old_site_runs:
            table_name = 'sites_%d' % old_run
            self._cache_db.query('DROP TABLE IF EXISTS `%s`' % table_name)

        for old_run in set(old_replica_runs) & set(old_site_runs):
            srun = '%09d' % old_run
            db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_spool_dir, old_run)
            if os.path.exists(db_file_name):
                try:
                    os.unlink(db_file_name)
                except:
                    LOG.error('Failed to delete %s' % db_file_name)
                    pass

        self._cache_db.query('DELETE FROM `replica_snapshot_usage` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL 1 WEEK)')
        self._cache_db.query('OPTIMIZE TABLE `replica_snapshot_usage`')
        self._cache_db.query('DELETE FROM `site_snapshot_usage` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL 1 WEEK)')
        self._cache_db.query('OPTIMIZE TABLE `site_snapshot_usage`')
