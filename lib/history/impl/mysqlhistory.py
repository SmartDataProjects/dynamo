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
from dynamo.dataformat import Site
from dynamo.dataformat.history import HistoryRecord, CopiedReplica, DeletedReplica

LOG = logging.getLogger(__name__)

class MySQLHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self, config):
        TransactionHistoryInterface.__init__(self, config)

        self._mysql = MySQL(config.db_params)

    def new_copy_cycle(self, partition, policy_version, comment = ''): #override
        return self._do_new_cycle(HistoryRecord.OP_COPY, partition, policy_version, comment)

    def new_deletion_cycle(self, partition, policy_version, comment = ''): #override
        return self._do_new_cycle(HistoryRecord.OP_DELETE, partition, policy_version, comment)

    def close_copy_cycle(self, cycle_number): #override
        self._do_close_cycle(HistoryRecord.OP_COPY, cycle_number)

    def close_deletion_cycle(self, cycle_number): #override
        self._do_close_cycle(HistoryRecord.OP_DELETE, cycle_number)

    def make_copy_entry(self, cycle_number, site, operation_id, approved, dataset_list): #override
        if self.test or self.read_only or cycle_number == 0:
            # Don't do anything
            return

        # site and datasets are expected to be already in the database.

        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = dict(self._mysql.select_many('datasets', ('name', 'id'), 'name', (d.name for d, s in dataset_list)))

        self._mysql.query('INSERT INTO `copy_requests` (`id`, `cycle_id`, `timestamp`, `approved`, `site_id`) VALUES (%s, %s, NOW(), %s, %s)', operation_id, cycle_number, approved, site_id)
        
        fields = ('copy_id', 'dataset_id', 'size')
        mapping = lambda (dataset, size): (operation_id, dataset_ids[dataset.name], size)

        self._mysql.insert_many('copied_replicas', fields, mapping, dataset_list, do_update = False)

    def make_deletion_entry(self, cycle_number, site, operation_id, approved, dataset_list): #override
        if self.test or self.read_only or cycle_number == 0:
            # Don't do anything
            return

        # site and datasets are expected to be already in the database.

        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = dict(self._mysql.select_many('datasets', ('name', 'id'), 'name', (d.name for d, s in dataset_list)))

        self._mysql.query('INSERT INTO `deletion_requests` (`id`, `cycle_id`, `timestamp`, `approved`, `site_id`) VALUES (%s, %s, NOW(), %s, %s)', operation_id, cycle_number, approved, site_id)

        fields = ('deletion_id', 'dataset_id', 'size')
        mapping = lambda (dataset, size): (operation_id, dataset_ids[dataset.name], size)

        self._mysql.insert_many('deleted_replicas', fields, mapping, dataset_list, do_update = False)

    def update_copy_entry(self, copy_record): #override
        if self.test or self.read_only:
            return

        self._mysql.query('UPDATE `copy_requests` SET `approved` = %s WHERE `id` = %s', copy_record.approved, copy_record.operation_id)

        sql = 'UPDATE `copied_replicas` SET `size` = %s, `status` = %s'
        sql += ' WHERE `copy_id` = %s AND `dataset_id` = (SELECT `id` FROM `datasets` WHERE `name` = %s)'
        for replica in copy_record.replicas:
            self._mysql.query(sql, replica.size, replica.status, copy_record.operation_id, replica.dataset_name)

    def update_deletion_entry(self, deletion_record): #override
        if self.test or self.read_only:
            return

        self._mysql.query('UPDATE `deletion_requests` SET `approved` = %s, WHERE `id` = %s', deletion_record.approved, deletion_record.operation_id)

        sql = 'UPDATE `deleted_replicas` SET `size` = %s'
        sql += ' WHERE `deletion_id` = %s AND `dataset_id` = (SELECT `id` FROM `datasets` WHERE `name` = %s)'
        for replica in deletion_record.replicas:
            self._mysql.query(sql, replica.size, deletion_record.operation_id, replica.dataset_name)

    def save_sites(self, sites): #override
        if self.read_only:
            return

        mapping = lambda s: (s.name,)
        self._mysql.insert_many('sites', ('name',), mapping, sites, do_update = True)

    def save_datasets(self, datasets): #override
        if self.read_only:
            return

        mapping = lambda d: (d.name,)
        self._mysql.insert_many('datasets', ('name',), mapping, datasets, do_update = True)

    def get_incomplete_copies(self, partition): #override
        sql = 'SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, d.`name`, s.`name`, c.`size`'
        sql += ' FROM `copied_replicas` AS c'
        sql += ' INNER JOIN `copy_requests` AS h ON h.`id` = c.`copy_id`'
        sql += ' INNER JOIN `cycles` AS r ON r.`id` = h.`cycle_id`'
        sql += ' INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = h.`site_id`'
        sql += ' WHERE h.`id` > 0 AND p.`name` LIKE \'%s\' AND c.`status` = \'enroute\' AND h.`cycle_id` > 0' % partition
        sql += ' ORDER BY h.`id`'

        records = []

        _copy_id = 0
        record = None
        for copy_id, timestamp, approved, dataset_name, site_name, size in self._mysql.xquery(sql):
            if copy_id != _copy_id:
                _copy_id = copy_id
                record = HistoryRecord(HistoryRecord.OP_COPY, copy_id, site_name, timestamp = timestamp, approved = approved)
                records.append(record)

            record.replicas.append(CopiedReplica(dataset_name = dataset_name, size = size, status = HistoryRecord.ST_ENROUTE))

        return records

    def get_site_name(self, operation_id): #override
        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `copy_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `deletion_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        return ''

    def get_deletion_cycles(self, partition, first = -1, last = -1): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return []

        partition_id = result[0]

        sql = 'SELECT `id` FROM `cycles` WHERE `partition_id` = %d AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'deletion\', \'deletion_test\')' % partition_id

        if first >= 0:
            sql += ' AND `id` >= %d' % first
        if last >= 0:
            sql += ' AND `id` <= %d' % last

        sql += ' ORDER BY `id` ASC'

        result = self._mysql.query(sql)

        if first < 0 and len(result) > 1:
            result = result[-1:]

        return result

    def get_copy_cycles(self, partition, first = -1, last = -1): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return []

        partition_id = result[0]

        sql = 'SELECT `id` FROM `cycles` WHERE `partition_id` = %d AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'copy\', \'copy_test\')' % partition_id

        if first >= 0:
            sql += ' AND `id` >= %d' % first
        if last >= 0:
            sql += ' AND `id` <= %d' % last

        sql += ' ORDER BY `id` ASC'

        if first < 0 and len(result) > 1:
            result = result[-1:]

        return result

    def get_cycle_timestamp(self, cycle_number): #override
        result = self._mysql.query('SELECT UNIX_TIMESTAMP(`time_start`) FROM `cycles` WHERE `id` = %s', cycle_number)
        if len(result) == 0:
            return 0

        return result[0]

    def _do_new_cycle(self, operation, partition, policy_version, comment):
        if self.read_only:
            return 0

        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
            part_id = self._mysql.last_insert_id
        else:
            part_id = part_ids[0]

        if operation == HistoryRecord.OP_COPY:
            if self.test:
                operation_str = 'copy_test'
            else:
                operation_str = 'copy'
        else:
            if self.test:
                operation_str = 'deletion_test'
            else:
                operation_str = 'deletion'

        self._mysql.query('INSERT INTO `cycles` (`operation`, `partition_id`, `policy_version`, `comment`, `time_start`) VALUES (%s, %s, %s, %s, NOW())', operation_str, part_id, policy_version, comment)

        return self._mysql.last_insert_id

    def _do_close_cycle(self, operation, cycle_number):
        if self.read_only:
            return

        self._mysql.query('UPDATE `cycles` SET `time_end` = FROM_UNIXTIME(%s) WHERE `id` = %s', time.time(), cycle_number)
