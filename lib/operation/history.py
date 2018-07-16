import time

from dynamo.history.history import HistoryDatabase
from dynamo.dataformat.history import HistoryRecord
from dynamo.utils.interface.mysql import MySQL

class DeletionHistoryDatabase(HistoryDatabase):
    """
    Interface to the deletion history database.
    """

    def make_entry(self, site_name):
        if self._read_only:
            operation_id = 0
        else:
            site_id = self.save_sites([site_name], get_ids = True)[0]
            operation_id = self.db.insert_get_id('deletion_operations', columns = ('timestamp', 'site_id'), values = (MySQL.bare('NOW()'), site_id))

        return HistoryRecord(HistoryRecord.OP_DELETE, operation_id, site_name, int(time.time()))

    def update_entry(self, deletion_record):
        if self._read_only:
            return

        dataset_names = [r.dataset_name for r in deletion_record.replicas]
        self.save_datasets(dataset_names)

        dataset_id_map = dict(self.db.select_many('datasets', ('name', 'id'), 'name', dataset_names))

        fields = ('deletion_id', 'dataset_id', 'size')
        mapping = lambda replica: (deletion_record.operation_id, dataset_id_map[replica.dataset_name], replica.size)

        self.db.insert_many('deleted_replicas', fields, mapping, deletion_record.replicas, do_update = True, update_columns = ('size',))

    def get_site_name(self, operation_id):
        sql = 'SELECT s.name FROM `sites` AS s INNER JOIN `deletion_operations` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s'
        result = self.db.query(sql, operation_id)
        if len(result) != 0:
            return result[0]

        return ''


class CopyHistoryDatabase(HistoryDatabase):
    """
    Interface to the copy history database.
    """

    def make_entry(self, site_name):
        if self._read_only:
            operation_id = 0
        else:
            site_id = self.save_sites([site_name], get_ids = True)[0]
            operation_id = self.db.insert_get_id('copy_operations', columns = ('timestamp', 'site_id'), values = (MySQL.bare('NOW()'), site_id))

        return HistoryRecord(HistoryRecord.OP_COPY, operation_id, site_name, int(time.time()))

    def update_entry(self, copy_record):
        if self._read_only:
            return

        dataset_names = [r.dataset_name for r in copy_record.replicas]
        self.save_datasets(dataset_names)

        dataset_id_map = dict(self.db.select_many('datasets', ('name', 'id'), 'name', dataset_names))

        fields = ('copy_id', 'dataset_id', 'size', 'status')
        mapping = lambda replica: (copy_record.operation_id, dataset_id_map[replica.dataset_name], replica.size, replica.status)

        self.db.insert_many('copied_replicas', fields, mapping, copy_record.replicas, do_update = True, update_columns = ('size', 'status'))

    def get_site_name(self, operation_id):
        sql = 'SELECT s.name FROM `sites` AS s INNER JOIN `copy_operations` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s'
        result = self.db.query(sql, operation_id)
        if len(result) != 0:
            return result[0]

        return ''
