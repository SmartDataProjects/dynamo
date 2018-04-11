import os
import sqlite3
import lzma
import logging

from dynamo.utils.interface import MySQL

LOG = logging.getLogger(__name__)

class DetoxHistory(object):
    """
    Class for handling Detox history.
    """

    def __init__(self, config):
        self._mysql = MySQL(config.get('db_params', None))
        self.history_db = config.history_db
        self.cache_db = config.cache_db
        self.snapshots_spool_dir = config.snapshots_spool_dir
        self.snapshots_archive_dir = config.snapshots_archive_dir

    def get_sites(self, cycle_number):
        """
        Collect the site status for a given cycle number or the latest cycle of the partition
        and return as a plain dict.
        @param cycle_number   Detox cycle number

        @return {site_name:  (status, quota)}
        """

        self._fill_snapshot_cache('sites', cycle_number)

        table_name = 'sites_%d' % cycle_number

        sql = 'SELECT s.`name`, n.`status`, n.`quota` FROM `%s`.`%s` AS n' % (self.cache_db, table_name)
        sql += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = n.`site_id`' % self.history_db

        sites_dict = {}

        for site_name, status, quota in self._mysql.xquery(sql):
            sites_dict[site_name] = (status, quota)

        return sites_dict

    def save_conditions(self, policy_lines):
        """
        Save policy conditions and set condition_ids.
        @param policy_lines  List of PolicyLine objects
        """

        for line in policy_lines:
            text = re.sub('\s+', ' ', line.condition.text)
            ids = self._mysql.query('SELECT `id` FROM `policy_conditions` WHERE `text` = %s', text)
            if len(ids) == 0:
                line.condition_id = self._mysql.query('INSERT INTO `policy_conditions` (`text`) VALUES (%s)', text)
            else:
                line.condition_id = ids[0]

    def save_decisions(self, cycle_number, deleted_list, kept_list, protected_list):
        """
        Save decisions and their reasons for all replicas.
        @param cycle_number      Cycle number.
        @param deleted_list    {replica: [([block_replica], condition)]}
        @param kept_list       {replica: [([block_replica], condition)]}
        @param protected_list  {replica: [([block_replica], condition)]}

        Note that in case of block-level operations, one dataset replica can appear
        in multiple of deleted, kept, and protected.
        """

        self._mysql.use_db(self.cache_db)

        # Make a snapshot table first and then fill the SQLite tables
        table_name = 'replicas_%s' % cycle_number

        if self._mysql.table_exists(table_name):
            self._mysql.query('DROP TABLE `%s`' % table_name)

        self._mysql.query('CREATE TABLE `%s` LIKE `replicas`' % table_name)

        # Insert full data into a temporary table with site and dataset names
        sql = 'CREATE TEMPORARY TABLE `replicas_tmp` ('
        sql += '`site` varchar(32) NOT NULL,'
        sql += '`dataset` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,'
        sql += '`size` bigint(20) unsigned NOT NULL,'
        sql += '`decision` enum(\'delete\',\'keep\',\'protect\') NOT NULL,'
        sql += '`condition` int(10) unsigned NOT NULL,'
        sql += 'KEY `site_dataset` (`site`,`dataset`)'
        sql += ')'
        self._mysql.query(sql)

        def replica_entry(entries, decision):
            for replica, matches in entries.iteritems():
                site_name = replica.site.name
                dataset_name = replica.dataset.name
                for condition_id, block_replicas in matches.iteritems():
                    size = sum(r.size for r in block_replicas)
                    yield (size, decision, condition_id, site_name, dataset_name)

        fields = ('site', 'dataset', 'size', 'decision', 'condition')
        self._mysql.insert_many('replicas_tmp', fields, None, replica_entry(deleted_list, 'delete'), do_update = False)
        self._mysql.insert_many('replicas_tmp', fields, None, replica_entry(kept_list, 'keep'), do_update = False)
        self._mysql.insert_many('replicas_tmp', fields, None, replica_entry(protected_list, 'protect'), do_update = False)

        # Then use insert select join to convert the names to ids
        sql = 'INSERT INTO `%s` (`site_id`, `dataset_id`, `size`, `decision`, `condition`)' % table_name
        sql += ' SELECT s.`id`, d.`id`, r.`size`, r.`decision`, r.`condition` FROM `replicas_tmp` AS r'
        sql += ' INNER JOIN `%s`.`sites` AS s ON s.`name` = r.`site`' % self.history_db
        sql += ' INNER JOIN `%s`.`datasets` AS d ON d.`name` = r.`dataset`' % self.history_db
        self._mysql.query(sql)

        self._mysql.query('DROP TABLE `replicas_tmp`')

        # Now transfer data to an SQLite file
        try:
            cycle_number += 0
        except TypeError:
            # cycle_number is actually a partition name
            db_file_name = '%s/snapshot_%s.db' % (self.snapshots_spool_dir, cycle_number)
        else:
            # Saving deletion decisions of a cycle
            db_file_name = '%s/snapshot_%09d.db' % (self.snapshots_spool_dir, cycle_number)

        try:
            os.makedirs(self.snapshots_spool_dir)
            os.chmod(self.snapshots_spool_dir, 0777)
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

        for entry in self._mysql.xquery('SELECT (`site_id`, `dataset_id`, `size`, `decision`, `condition`) FROM `%s`' % table_name):
            snapshot_cursor(sql, entry)

        snapshot_db.commit()
        
        snapshot_cursor.close()
        snapshot_db.close()

        os.chmod(db_file_name, 0666)

    def save_siteinfo(self, cycle_number, quotas):
        """
        Save the site partition quotas and site statuses for the cycle.
        @param cycle_number   Cycle number.
        @param quotas         {site: quota in TB}
        """

        self._mysql.use_db(self.cache_db)

        # Make a snapshot table first and then fill the SQLite tables
        table_name = 'sites_%s' % cycle_number

        if self._mysql.table_exists(table_name):
            self._mysql.query('DROP TABLE `%s`' % table_name)

        self._mysql.query('CREATE TABLE `%s` LIKE `sites`' % table_name)

        # Insert full data into a temporary table with site and dataset names
        sql = 'CREATE TEMPORARY TABLE `sites_tmp` ('
        sql += '`site` varchar(32) NOT NULL,'
        sql += '`status` enum(\'ready\',\'waitroom\',\'morgue\',\'unknown\') NOT NULL,'
        sql += '`quota` int(10) NOT NULL,'
        sql += 'KEY `site` (`site`)'
        sql += ')'
        self._mysql.query(sql)

        fields = ('site', 'status', 'quota')
        mapping = lambda (site, quota): (site.name, site.status, quota)
        self._mysql.insert_many('sites_tmp', fields, mapping, quotas.iteritems(), do_update = False)

        # Then use insert select join to convert the names to ids
        sql = 'INSERT INTO `%s` (`site_id`, `status`, `quota`)' % table_name
        sql += ' SELECT s.`id`, t.`status`, t.`quota` FROM `sites_tmp` AS t'
        sql += ' INNER JOIN `%s`.`sites` AS s ON s.`name` = t.`site`' % self.history_db
        self._mysql.query(sql)

        self._mysql.query('DROP TABLE `sites_tmp`')

        # Now transfer data to an SQLite file
        try:
            cycle_number += 0
        except TypeError:
            # cycle_number is actually the partition name
            db_file_name = '%s/snapshot_%s.db' % (self.snapshots_spool_dir, cycle_number)
            is_cycle = False
        else:
            # Saving quotas during a cycle
            db_file_name = '%s/snapshot_%09d.db' % (self.snapshots_spool_dir, cycle_number)
            is_cycle = True

        # DB file should exist already - this function is called after save_deletion_decisions

        snapshot_db = sqlite3.connect(db_file_name)
        snapshot_cursor = snapshot_db.cursor()

        sql = 'CREATE TABLE `statuses` ('
        sql += '`id` TINYINT PRIMARY KEY NOT NULL,'
        sql += '`value` TEXT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'ready\')' % Site.STAT_READY)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'waitroom\')' % Site.STAT_WAITROOM)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'morgue\')' % Site.STAT_MORGUE)
        snapshot_db.execute('INSERT INTO `statuses` VALUES (%d, \'unknown\')' % Site.STAT_UNKNOWN)

        sql = 'CREATE TABLE `sites` ('
        sql += '`site_id` SMALLINT PRIMARY KEY NOT NULL,'
        sql += '`status_id` TINYINT NOT NULL REFERENCES `statuses`(`id`),'
        sql += '`quota` INT NOT NULL'
        sql += ')'
        snapshot_db.execute(sql)

        sql = 'INSERT INTO `sites` VALUES (?, ?, ?)'

        for site, quota in quotas.iteritems():
            snapshot_cursor.execute(sql, (site_id_map[site.name], site.status, quota))

        snapshot_db.commit()

        snapshot_cursor.close()
        snapshot_db.close()

        if is_cycle:
            # This was a numbered cycle
            # Archive the sqlite3 file
            # Relying on the fact save_quotas is called after save_deletion_decisions
    
            scycle = '%09d' % cycle_number
            archive_dir_name = '%s/%s/%s' % (self.snapshots_archive_dir, scycle[:3], scycle[3:6])
            xz_file_name = '%s/snapshot_%09d.db.xz' % (archive_dir_name, cycle_number)
    
            try:
                os.makedirs(archive_dir_name)
            except OSError:
                pass
    
            with open(db_file_name, 'rb') as db_file:
                with open(xz_file_name, 'wb') as xz_file:
                    xz_file.write(lzma.compress(db_file.read()))

            self._update_cache_usage('replicas', cycle_number)
            self._update_cache_usage('sites', cycle_number)

    def get_deletion_decisions(self, cycle_number, size_only = True):
        """
        @param cycle_number   Cycle number
        @param size_only      Boolean
        
        @return If size_only = True: a dict {site: (protect_size, delete_size, keep_size)}
                If size_only = False: a massive dict {site: [(dataset, size, decision, reason)]}
        """

        self._fill_snapshot_cache('replicas', cycle_number)

        table_name = 'replicas_%d' % cycle_number

        if size_only:
            # return {site_name: (protect_size, delete_size, keep_size)}
            volumes = {}
            sites = set()

            query = 'SELECT s.`name`, SUM(r.`size`) * 1.e-12 FROM `%s`.`%s` AS r' % (self.cache_db, table_name)
            query += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = r.`site_id`' % self.history_db
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

            query = 'SELECT s.`name`, d.`name`, r.`size`, r.`decision`, p.`text` FROM `%s`.`%s` AS r' % (self.cache_db, table_name)
            query += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = r.`site_id`' % self.history_db
            query += ' INNER JOIN `%s`.`datasets` AS d ON d.`id` = r.`dataset_id`' % self.history_db
            query += ' LEFT JOIN `%s`.`policy_conditions` AS p ON p.`id` = r.`condition`' % self.history_db
            query += ' ORDER BY s.`name` ASC, r.`size` DESC'

            product = {}

            _site_name = ''

            for site_name, dataset_name, size, decision, reason in self._mysql.xquery(query):
                if site_name != _site_name:
                    product[site_name] = []
                    current = product[site_name]
                    _site_name = site_name
                
                current.append((dataset_name, size, decision, reason))

            return product

    def _fill_snapshot_cache(self, template, cycle_number, overwrite = False):
        self._mysql.use_db(self.cache_db)

        # cycle_number is either a cycle number or a partition name. %s works for both
        table_name = '%s_%s' % (template, cycle_number)

        table_exists = self._mysql.table_exists(table_name)

        if overwrite or not table_exists:
            # fill from sqlite
            if table_exists:
                self._mysql.query('TRUNCATE TABLE `%s`' % table_name)
            else:
                self._mysql.query('CREATE TABLE `%s` LIKE `%s`' % (table_name, template))

            try:
                cycle_number += 0
            except TypeError:
                db_file_name = '%s/snapshot_%s.db' % (self.config.snapshots_spool_dir, cycle_number)

                if not os.path.exists(db_file_name):
                    return
            else:
                db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_spool_dir, cycle_number)

                if not os.path.exists(db_file_name):
                    scycle = '%09d' % cycle_number
                    xz_file_name = '%s/%s/%s/snapshot_%09d.db.xz' % (self.config.snapshots_archive_dir, scycle[:3], scycle[3:6], cycle_number)
                    if not os.path.exists(xz_file_name):
                        raise RuntimeError('Snapshot DB ' + db_file_name + ' does not exist')
    
                    with open(xz_file_name, 'rb') as xz_file:
                        with open(db_file_name, 'wb') as db_file:
                            db_file.write(lzma.decompress(xz_file.read()))

            snapshot_db = sqlite3.connect(db_file_name)
            snapshot_db.text_factory = str # otherwise we'll get unicode and MySQLdb cannot convert that
            snapshot_cursor = snapshot_db.cursor()

            def make_snapshot_reader():
                if template == 'replicas':
                    sql = 'SELECT r.`site_id`, r.`dataset_id`, r.`size`, d.`value`, r.`condition` FROM `replicas` AS r'
                    sql += ' INNER JOIN `decisions` AS d ON d.`id` = r.`decision_id`'
                elif template == 'sites':
                    sql = 'SELECT s.`site_id`, t.`value`, s.`quota` FROM `sites` AS s'
                    sql += ' INNER JOIN `statuses` AS t ON t.`id` = s.`status_id`'
                    
                snapshot_cursor.execute(sql)
                
                while True:
                    row = snapshot_cursor.fetchone()
                    if row is None:
                        return

                    yield row

            snapshot_reader = make_snapshot_reader()

            if template == 'replicas':
                fields = ('site_id', 'dataset_id', 'size', 'decision', 'condition')
            elif template == 'sites':
                fields = ('site_id', 'status', 'quota')
                
            self._mysql.insert_many(table_name, fields, None, snapshot_reader, do_update = False)

            snapshot_cursor.close()
            snapshot_db.close()

        try:
            cycle_number += 0
        except TypeError:
            # cycle_number is actually a partition name. Nothing more to do
            return
        else:
            # cycle_number is really a number. Update the partition cache table too
            sql = 'SELECT p.`name` FROM `partitions` AS p INNER JOIN `cycles` AS r ON r.`partition_id` = p.`id` WHERE r.`id` = %s'
            partition = self._mysql.query(sql, cycle_number)[0]
    
            self._fill_snapshot_cache(template, partition, overwrite)

            # then update the cache usage
            self._update_cache_usage(template, cycle_number)

    def _update_cache_usage(self, template, cycle_number):
        self._mysql.use_db(self.cache_db)

        self._mysql.query('INSERT INTO `{template}_snapshot_usage` VALUES (%s, NOW())'.format(template = template), cycle_number)

        # clean old cache
        sql = 'SELECT `cycle_id` FROM (SELECT `cycle_id`, MAX(`timestamp`) AS m FROM `replicas_snapshot_usage` GROUP BY `cycle_id`) AS t WHERE m < DATE_SUB(NOW(), INTERVAL 1 WEEK)'
        old_replica_cycles = self._mysql.query(sql)
        for old_cycle in old_replica_cycles:
            table_name = 'replicas_%d' % old_cycle
            self._mysql.query('DROP TABLE IF EXISTS `%s`' % table_name)

        sql = 'SELECT `cycle_id` FROM (SELECT `cycle_id`, MAX(`timestamp`) AS m FROM `sites_snapshot_usage` GROUP BY `cycle_id`) AS t WHERE m < DATE_SUB(NOW(), INTERVAL 1 WEEK)'
        old_site_cycles = self._mysql.query(sql)
        for old_cycle in old_site_cycles:
            table_name = 'sites_%d' % old_cycle
            self._mysql.query('DROP TABLE IF EXISTS `%s`' % table_name)

        for old_cycle in set(old_replica_cycles) & set(old_site_cycles):
            scycle = '%09d' % old_cycle
            db_file_name = '%s/snapshot_%09d.db' % (self.config.snapshots_spool_dir, old_cycle)
            if os.path.exists(db_file_name):
                try:
                    os.unlink(db_file_name)
                except:
                    LOG.error('Failed to delete %s' % db_file_name)
                    pass

        self._mysql.query('DELETE FROM `replicas_snapshot_usage` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL 1 WEEK)')
        self._mysql.query('OPTIMIZE TABLE `replicas_snapshot_usage`')
        self._mysql.query('DELETE FROM `sites_snapshot_usage` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL 1 WEEK)')
        self._mysql.query('OPTIMIZE TABLE `sites_snapshot_usage`')
