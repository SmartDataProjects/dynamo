from dynamo.operation.history import CopyHistoryDatabase
from dynamo.dataformat.history import HistoryRecord
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class DealerHistoryBase(CopyHistoryDatabase):
    """
    Parts of the DealerHistory that can be used by the web dealer monitor.
    """

    def __init__(self, config):
        OperationHistoryDatabase.__init__(self, config)

    def get_incomplete_copies(self, partition):
        """
        Get a list of incomplete copies.
        @param partition   partition name

        @return list of HistoryRecords
        """

        sql = 'SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, d.`name`, s.`name`, c.`size`'
        sql += ' FROM `copied_replicas` AS c'
        sql += ' INNER JOIN `copy_operations` AS h ON h.`id` = c.`copy_id`'
        sql += ' INNER JOIN `cycle_copy_operations` AS cc ON cc.`operation_id` = c.`id`'
        sql += ' INNER JOIN `cycles` AS r ON r.`id` = cc.`cycle_id`'
        sql += ' INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = h.`site_id`'
        sql += ' WHERE h.`id` > 0 AND p.`name` LIKE \'%s\' AND c.`status` = \'enroute\' AND cc.`cycle_id` > 0' % partition
        sql += ' ORDER BY h.`id`'

        records = []

        _copy_id = 0
        record = None
        for copy_id, timestamp, approved, dataset_name, site_name, size in self.db.xquery(sql):
            if copy_id != _copy_id:
                _copy_id = copy_id
                record = HistoryRecord(HistoryRecord.OP_COPY, copy_id, site_name, timestamp = timestamp, approved = approved)
                records.append(record)

            record.replicas.append(CopiedReplica(dataset_name = dataset_name, size = size, status = HistoryRecord.ST_ENROUTE))

        return records

    def get_cycles(self, partition, first = -1, last = -1):
        """
        Get a list of copy cycles in range first <= cycle <= last. If first == -1, pick only the latest before last.
        If last == -1, select cycles up to the latest.
        @param partition  partition name
        @param first      first cycle
        @param last       last cycle

        @return list of cycle numbers
        """
        result = self.db.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
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


class DealerHistory(DealerHistoryBase):
    def new_cycle(self, partition, policy_version, comment = '', test = False):
        """
        Set up a new copy cycle for the partition.
        @param partition        partition name string
        @param policy_version   string for policy version
        @param comment          comment string
        @param test             if True, create a copy_test cycle.

        @return cycle number.
        """

        if self.read_only:
            return 0

        part_id = self.save_partitions([partition], get_ids = True)[0]

        if test:
            operation_str = 'copy_test'
        else:
            operation_str = 'copy'

        columns = ('operation', 'partition_id', 'policy_version', 'comment', 'time_start')
        values = (operation_str, part_id, policy_version, comment, MySQL.bare('NOW()'))
        return self.db.insert_get_id('cycles', columns = columns, values = values)

    def close_cycle(self, cycle_number):
        """
        Finalize the records for the given cycle.
        @param cycle_number   Cycle number
        """

        if self.read_only:
            return

        self.db.query('UPDATE `cycles` SET `time_end` = FROM_UNIXTIME(%s) WHERE `id` = %s', time.time(), cycle_number)

    def make_cycle_entry(self, cycle_id, site):
        history_record = self.make_entry(site)

        self.db.query('INSERT INTO `cycle_copy_operations` (`cycle_id`, `operation_id`) VALUES (%s, %s)', cycle_number, history_record.operation_id)

        return history_record
