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
from dynamo.dataformat import Site, HistoryRecord

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

    def make_copy_entry(self, cycle_number, site, operation_id, approved, datasets, size): #override
        if self.test or cycle_number == 0:
            # Don't do anything
            return

        # site and datasets are expected to be already in the database.

        sql = 'INSERT INTO `copy_requests` (`id`, `cycle_id`, `timestamp`, `approved`, `site_id`, `size`)'
        sql += ' SELECT %s, %s, NOW(), %s, `id`, %s FROM `sites` WHERE `name` = %s'
        self._mysql.query(sql, operation_id, cycle_number, approved, size, site.name)

        def dataset_name():
            for dataset in datasets:
                yield dataset.name

        sql = 'INSERT INTO `copied_replicas` (`copy_id`, `dataset_id`) SELECT %d, `id` FROM `datasets`'
        self._mysql.execute_many(sql, 'name', dataset_name())

    def make_deletion_entry(self, cycle_number, site, operation_id, approved, datasets, size): #override
        if self.test or cycle_number == 0:
            # Don't do anything
            return

        # site and datasets are expected to be already in the database (save_deletion_decisions should be called first).

        sql = 'INSERT INTO `deletion_requests` (`id`, `cycle_id`, `timestamp`, `approved`, `site_id`, `size`)'
        sql += ' SELECT %s, %s, NOW(), %s, `id`, %s FROM `sites` WHERE `name` = %s'
        self._mysql.query(sql, operation_id, cycle_number, approved, size, site.name)

        def dataset_name():
            for dataset in datasets:
                yield dataset.name

        sql = 'INSERT INTO `deleted_replicas` (`deletion_id`, `dataset_id`) SELECT %d, `id` FROM `datasets`'
        self._mysql.execute_many(sql, 'name', dataset_name())

    def update_copy_entry(self, copy_record): #override
        # copy_record status: INPROGRESS -> 0, COMPLETE -> 1, CANCELLED -> 2
        sql = 'UPDATE `copy_requests` SET `approved` = %s, `size` = %s, `completed` = %s WHERE `id` = %s'
        self._mysql.query(sql, copy_record.approved, copy_record.size, copy_record.status, copy_record.operation_id)

    def update_deletion_entry(self, deletion_record): #override
        sql = 'UPDATE `deletion_requests` SET `approved` = %s, `size` = %s WHERE `id` = %s'
        self._mysql.query(sql, deletion_record.approved, deletion_record.size, deletion_record.operation_id)

    def save_sites(self, sites): #override
        mapping = lambda s: s.name
        self._mysql.insert_many('sites', ('name',), mapping, sites, do_update = True)

    def save_datasets(self, datasets): #override
        mapping = lambda d: d.name
        self._mysql.insert_many('datasets', ('name',), mapping, datasets, do_update = True)

    def get_incomplete_copies(self, partition): #override
        query = 'SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`'
        query += ' FROM `copy_requests` AS h'
        query += ' INNER JOIN `cycles` AS r ON r.`id` = h.`cycle_id`'
        query += ' INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id`'
        query += ' INNER JOIN `sites` AS s ON s.`id` = h.`site_id`'
        query += ' WHERE h.`id` > 0 AND p.`name` LIKE \'%s\' AND h.`completed` = 0 AND h.`cycle_id` > 0' % partition
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
        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            part_id = self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
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

        return self._mysql.query('INSERT INTO `cycles` (`operation`, `partition_id`, `policy_version`, `comment`, `time_start`) VALUES (%s, %s, %s, %s, NOW())', operation_str, part_id, policy_version, comment)

    def _do_close_cycle(self, operation, cycle_number):
        self._mysql.query('UPDATE `cycles` SET `time_end` = FROM_UNIXTIME(%s) WHERE `id` = %s', time.time(), cycle_number)
