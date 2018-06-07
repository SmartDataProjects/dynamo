import sys
import collections
import logging

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery
from dynamo.utils.interface.mysql import MySQL

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

    def form_batches(self, tasks): #override
        if len(tasks) == 0:
            return []

        if hasattr(tasks[0], 'source'):
            # These are transfer tasks
            # GFAL2 has a "bulk-copy" feature which supposedly is more efficient than copying one by one
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
        fields = ('id', 'source', 'destination')
        def mapping(task):
            lfn = task.subscription.file.lfn
            return (
                task.id,
                task.source.to_pfn(lfn, 'gfal2'),
                task.subscription.destination.to_pfn(lfn, 'gfal2')
            )

        if not self.dry_run:
            self.db.insert_many('standalone_transfer_queue', fields, mapping, batch_tasks)

        LOG.debug('Inserted %d entries to standalone_transfer_queue for batch %d.', len(batch_tasks), batch_id)

        return True

    def start_deletions(self, batch_id, batch_tasks): #override
        fields = ('id', 'file')
        def mapping(task):
            lfn = task.subscription.file.lfn
            return (
                task.id,
                task.desubscription.site.to_pfn(lfn, 'gfal2')
            )

        if not self.dry_run:
            self.db.insert_many('standalone_deletion_queue', fields, mapping, batch_tasks)

        LOG.debug('Inserted %d entries to standalone_deletion_queue for batch %d.', len(batch_tasks), batch_id)

        return True

    def get_transfer_status(self, batch_id): #override
        return self._get_status(batch_id, 'transfer')

    def get_deletion_status(self, batch_id): #override
        return self._get_status(batch_id, 'deletion')

    def forget_transfer_status(self, batch_id, task_id): #override
        return self._forget_status(batch_id, task_id, 'transfer')

    def forget_deletion_status(self, batch_id, task_id): #override
        return self._forget_status(batch_id, task_id, 'deletion')

    def _get_status(self, batch_id, optype):
        sql = 'SELECT q.`id`, a.`status`, a.`exitcode`, UNIX_TIMESTAMP(a.`start_time`), UNIX_TIMESTAMP(a.`finish_time`) FROM `standalone_{op}_queue` AS a'
        sql += ' INNER JOIN `{op}_queue` AS q ON q.`id` = a.`id`'
        sql += ' WHERE q.`batch_id` = %s'
        sql = sql.format(op = optype)

        return [(i, FileQuery.status_val(s), c, t, f) for (i, s, c, t, f) in self.db.xquery(sql, batch_id)]

    def _forget_status(self, batch_id, task_id, optype):
        sql = 'DELETE FROM `standalone_{op}_queue` WHERE `id` = %s'
        sql = sql.format(op = optype)
        self.db.query(sql, task_id)
