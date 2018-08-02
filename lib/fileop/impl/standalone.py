import sys
import collections
import logging

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import File

LOG = logging.getLogger(__name__)

class StandaloneFileOperation(FileTransferOperation, FileTransferQuery, FileDeletionOperation, FileDeletionQuery):
    """
    Interface to in-house transfer & deletion daemon using MySQL for bookkeeping.
    """

    def __init__(self, config):
        FileTransferOperation.__init__(self, config)
        FileTransferQuery.__init__(self, config)
        FileDeletionOperation.__init__(self, config)
        FileDeletionQuery.__init__(self, config)

        self.db = MySQL(config.db_params)

    def num_pending_transfers(self): #override
        # FOD can throttle itself.
        return 0

    def num_pending_deletions(self): #override
        # FOD can throttle itself.
        return 0

    def form_batches(self, tasks): #override
        if len(tasks) == 0:
            return []

        if hasattr(tasks[0], 'source'):
            # These are transfer tasks
            by_endpoints = collections.defaultdict(list)
            for task in tasks:
                endpoints = (task.source, task.subscription.destination)
                by_endpoints[endpoints].append(task)

            return by_endpoints.values()
        else:
            by_endpoint = collections.defaultdict(list)
            for task in tasks:
                by_endpoint[task.desubscription.site].append(task)

            return by_endpoint.values()

    def start_transfers(self, batch_id, batch_tasks): #override
        if len(batch_tasks) == 0:
            return {}

        result = {}

        # tasks should all have the same source and destination
        source = batch_tasks[0].source
        destination = batch_tasks[0].subscription.destination

        fields = ('id', 'source', 'destination', 'checksum_algo', 'checksum')

        def yield_task_entry():
            for task in batch_tasks:
                lfile = task.subscription.file
                lfn = lfile.lfn
                source_pfn = source.to_pfn(lfn, 'gfal2')
                dest_pfn = destination.to_pfn(lfn, 'gfal2')

                if source_pfn is None or dest_pfn is None:
                    # either gfal2 is not supported or lfn could not be mapped
                    result[task] = False
                    continue

                if self.checksum_algorithm:
                    checksum = (self.checksum_algorithm, str(lfile.checksum[self.checksum_index]))
                else:
                    checksum = (None, None)

                result[task] = True
                yield (task.id, source_pfn, dest_pfn) + checksum

        if not self._read_only:
            sql = 'INSERT INTO `standalone_transfer_batches` (`batch_id`, `source_site`, `destination_site`) VALUES (%s, %s, %s)'
            self.db.query(sql, batch_id, source.name, destination.name)
            self.db.insert_many('standalone_transfer_tasks', fields, None, yield_task_entry())

        LOG.debug('Inserted %d entries to standalone_transfer_tasks for batch %d.', len(batch_tasks), batch_id)

        return result

    def start_deletions(self, batch_id, batch_tasks): #override
        if len(batch_tasks) == 0:
            return {}

        result = {}

        # tasks should all have the same target site
        site = batch_tasks[0].desubscription.site

        fields = ('id', 'file')

        def yield_task_entry():
            for task in batch_tasks:
                lfn = task.desubscription.file.lfn
                pfn = site.to_pfn(lfn, 'gfal2')

                if pfn is None:
                    # either gfal2 is not supported or lfn could not be mapped
                    result[task] = False
                    continue

                result[task] = True
                yield (task.id, pfn)

        if not self._read_only:
            sql = 'INSERT INTO `standalone_deletion_batches` (`batch_id`, `site`) VALUES (%s, %s)'
            self.db.query(sql, batch_id, site.name)
            self.db.insert_many('standalone_deletion_tasks', fields, None, yield_task_entry())

        LOG.debug('Inserted %d entries to standalone_deletion_tasks for batch %d.', len(batch_tasks), batch_id)

        return result

    def cancel_transfers(self, task_ids): #override
        return self._cancel(task_ids, 'transfer')

    def cancel_deletions(self, task_ids): #override
        return self._cancel(task_ids, 'deletion')

    def cleanup(self): #override
        sql = 'DELETE FROM f USING `standalone_transfer_tasks` AS f LEFT JOIN `transfer_tasks` AS t ON t.`id` = f.`id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `standalone_deletion_tasks` AS f LEFT JOIN `deletion_tasks` AS t ON t.`id` = f.`id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `standalone_transfer_batches` AS f LEFT JOIN `transfer_batches` AS t ON t.`id` = f.`batch_id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `standalone_deletion_batches` AS f LEFT JOIN `deletion_batches` AS t ON t.`id` = f.`batch_id` WHERE t.`id` IS NULL'
        self.db.query(sql)

        # Delete the source tasks - caution: wipes out all tasks when switching the operation backend
        sql = 'DELETE FROM t USING `transfer_tasks` AS t'
        sql += ' LEFT JOIN `standalone_transfer_tasks` AS f ON f.`id` = t.`id`'
        sql += ' WHERE f.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM t USING `deletion_tasks` AS t'
        sql += ' LEFT JOIN `standalone_deletion_tasks` AS f ON f.`id` = t.`id`'
        sql += ' WHERE f.`id` IS NULL'
        self.db.query(sql)

    def get_transfer_status(self, batch_id): #override
        return self._get_status(batch_id, 'transfer')

    def get_deletion_status(self, batch_id): #override
        return self._get_status(batch_id, 'deletion')

    def write_transfer_history(self, history_db, task_id, history_id): #override
        pass

    def write_deletion_history(self, history_db, task_id, history_id): #override
        pass

    def forget_transfer_status(self, task_id): #override
        return self._forget_status(task_id, 'transfer')

    def forget_deletion_status(self, task_id): #override
        return self._forget_status(task_id, 'deletion')

    def forget_transfer_batch(self, batch_id): #override
        return self._forget_batch(batch_id, 'transfer')

    def forget_deletion_batch(self, batch_id): #override
        return self._forget_batch(batch_id, 'deletion')

    def _cancel(self, task_ids, optype):
        sql = 'UPDATE `standalone_{op}_tasks` SET `status` = \'cancelled\''.format(op = optype)
        self.db.execute_many(sql, 'id', task_ids, ['`status` IN (\'new\', \'queued\')'])

    def _get_status(self, batch_id, optype):
        sql = 'SELECT q.`id`, a.`status`, a.`exitcode`, a.`message`, UNIX_TIMESTAMP(a.`start_time`), UNIX_TIMESTAMP(a.`finish_time`) FROM `standalone_{op}_tasks` AS a'
        sql += ' INNER JOIN `{op}_tasks` AS q ON q.`id` = a.`id`'
        sql += ' WHERE q.`batch_id` = %s'
        sql = sql.format(op = optype)

        return [(i, FileQuery.status_val(s), c, m, t, f) for (i, s, c, m, t, f) in self.db.xquery(sql, batch_id)]

    def _forget_status(self, task_id, optype):
        if self._read_only:
            return

        sql = 'DELETE FROM `standalone_{op}_tasks` WHERE `id` = %s'.format(op = optype)
        self.db.query(sql, task_id)

    def _forget_batch(self, batch_id, optype):
        if self._read_only:
            return

        sql = 'DELETE FROM `standalone_{op}_batches` WHERE `batch_id` = %s'
        self.db.query(sql.format(op = optype), batch_id)
