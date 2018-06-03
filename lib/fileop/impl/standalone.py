import sys
import collections
import logging

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
        self._start(batch_id, 'transfer')

    def start_deletions(self, batch_id, batch_tasks): #override
        self._start(batch_id, 'deletion')

    def get_transfer_status(self, batch_id): #override
        self._get_status(batch_id, 'transfer', FileTransferQuery)

    def get_deletion_status(self, batch_id): #override
        self._get_status(batch_id, 'deletion', FileDeletionQuery)

    def _start(self, batch_id, optype):
        sql = 'INSERT INTO `standalone_{op}_queue` (`id`)'
        sql += ' SELECT `id` FROM `{op}_queue` WHERE `batch_id` = %s'
        
        sql = sql.format(op = optype)

        self.db.query(sql, batch_id)

        return True

    def _get_status(self, batch_id, optype, cls):
        sql = 'SELECT q.`id`, a.`status`, a.`exitcode`, a.`finish_time` FROM `standalone_{op}_queue` AS a'
        sql += ' INNER JOIN `{op}_queue` AS q ON q.`id` = a.`id`'.format(op = optype)
        sql += ' WHERE q.`batch_id` = %s'

        sql = sql.format(op = optype)

        result = [(i, cls.status_val(s), c, t) for (i, s, c, t) in self.db.xquery(sql, batch_id)]

        # Delete failed and done entries
        sql = 'DELETE FROM s USING `standalone_{op}_queue` AS a'
        sql += ' INNER JOIN `{op}_queue` AS q ON q.`id` = a.`id`'.format(op = optype)
        sql += ' WHERE q.`batch_id` = %s AND a.`status` IN (\'done\', \'failed\')'

        self.db.query(sql)
