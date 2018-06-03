import sys
import time
import calendar
import logging

import fts3.rest.client.easy as fts3
from fts3.rest.client.request import Request

from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class FTSFileOperation(FileTransferOperation, FileTransferQuery, FileDeletionOperation, FileDeletionQuery):
    def __init__(self, config):
        FileTransferOperation.__init__(self, config)
        FileTransferQuery.__init__(self, config)
        FileDeletionOperation.__init__(self, config)
        FileDeletionQuery.__init__(self, config)

        self.server_url = config.fts_server
        self.server_id = 0 # server id in the DB

        # Parameter "retry" for fts3.new_job
        self.fts_retry = config.fts_retry

        # Bookkeeping device
        self.mysql = MySQL(config.db_params)

        # Cache the error messages
        self.error_messages = self.mysql.query('SELECT `code`, `message` FROM `fts_error_messages`')

        # Reuse the context object
        self.keep_context = config.get('keep_context', True)
        self._context = None

        self.dry_run = False

    def form_batches(self, tasks): #override
        if len(tasks) == 0:
            return []

        # FTS3 has no restriction on how to group the transfers
        batches = [[]]
        for task in tasks:
            batches[-1].append(task)
            if len(batches[-1]) == self.batch_size:
                batches.append([])

        return batches

    def start_transfers(self, batch_id, batch_tasks): #override
        transfers = []

        pfn_to_task = {}

        for task in batch_tasks:
            sub = task.subscription
            lfn = sub.file.lfn
            source_pfn = task.source.to_pfn(lfn, 'gfal2')
            dest_pfn = sub.destination.to_pfn(lfn, 'gfal2')

            transfers.append(fts3.new_transfer(source_pfn, dest_pfn))

            # there should be only one task per destination pfn
            pfn_to_task[dest_pfn] = task

        job = fts3.new_job(transfers, retry = self.fts_retry, overwrite = True, verify_checksum = False)

        return self._submit_job(job, 'transfer', batch_id)

    def start_deletions(self, batch_id, batch_tasks): #override
        pfn_to_task = {}

        for task in batch_tasks:
            desub = task.desubscription
            lfn = desub.file.lfn
            pfn = desub.site.to_pfn(lfn, 'gfal2')

            # there should be only one task per destination pfn
            pfn_to_task[dest_pfn] = task

        job = fts3.new_delete_job(pfn_to_task.keys())

        return self._submit_job(job, 'deletion', batch_id, pfn_to_task)

    def get_transfer_status(self, batch_id): #override
        if self.server_id == 0:
            self._set_server_id()

        return self._get_status(batch_id, 'transfer')

    def get_deletion_status(self, batch_id): #override
        if self.server_id == 0:
            self._set_server_id()

        return self._get_status(batch_id, 'transfer')

    def _ftscall(self, method, *args, **kwd):
        if self._context is None:
            # request_class = Request -> use "requests"-based https call (instead of default PyCURL, which may not be able to handle proxy certificates depending on the cURL installation)
            # verify = False -> do not verify the server certificate
            context = fts3.Context(self.server_url, request_class = Request, verify = False)

            if self.keep_context:
                self._context = context
        else:
            context = self._context

        return getattr(fts3, method)(context, *args, **kwd)

    def _submit_job(self, job, optype, batch_id, pfn_to_task):
        if self.dry_run:
            job_id = 'test'
        else:
            try:
                job_id = self._ftscall('submit', job)
            except:
                exc_type, exc, tb = sys.exc_info()
                LOG.error('Failed to submit %s to FTS: Exception %s (%s)', optype, exc_type.__name__, str(exc))
                return False

        # list of file-level operations (one-to-one with pfn)
        try:
            if optype == 'transfer':
                key = 'files'
            else:
                key = 'dm'

            fts_files = self._ftscall('get_job_status', job_id = job_id, list_files = True)[key]
        except:
            exc_type, exc, tb = sys.exc_info()
            LOG.error('Failed to get status of job %s from FTS: Exception %s (%s)', job_id, exc_type.__name__, str(exc))
            return False

        if self.server_id == 0:
            self._set_server_id()

        sql = 'INSERT INTO `fts_{op}_batches` (`batch_id`, `fts_server_id`, `job_id`)'.format(op = optype)
        sql += ' VALUES (%s, %s, %s)'
        if not self.dry_run:
            self.mysql.query(sql, batch_id, self.server_id, job_id)

        fields = (optype + '_id', 'batch_id', 'fts_file_id')
        mapping = lambda f: (pfn_to_task[f['dest_surl']].id, batch_id, f['file_id'])

        if not self.dry_run:
            self.mysql.insert_many('fts_' optype + '_files', fields, mapping, fts_files)

        return True

    def _get_status(self, batch_id, optype):
        sql = 'SELECT `job_id` FROM `fts_{optype}_batches`'
        sql += ' WHERE `fts_server_id` = %s, `batch_id` = %s'

        job_id = self.mysql.query(sql.format(optype = optype), self.server_id, batch_id)[0]

        sql = 'SELECT `fts_file_id`, `{optype}_id` FROM `fts_{optype}_files` WHERE `batch_id` = %s'
        fts_to_queue = dict(self.mysql.xquery(sql.format(optype = optype), batch_id))

        result = self._ftscall('get_job_status', job_id = job_id, list_files = True)

        if optype == 'transfer':
            fts_files = result['files']
            done = FileTransferQuery.STAT_DONE
            failed = FileTransferQuery.STAT_FAILED
            new = FileTransferQuery.STAT_NEW
            inprogress = FileTransferQuery.STAT_INPROGRESS
        else:
            fts_files = result['dm']
            done = FileDeletionQuery.STAT_DONE
            failed = FileDeletionQuery.STAT_FAILED
            new = FileDeletionQuery.STAT_NEW
            inprogress = FileDeletionQuery.STAT_INPROGRESS

        status = []

        for fts_file in fts_files:
            try:
                task_id = fts_to_queue[fts_file['file_id']]
            except KeyError:
                continue

            state = fts_file['file_state']

            if state == 'FINISHED':
                status = done
                exitcode = 0
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'FAILED':
                status = failed
                exitcode = -1
                for code, msg in self.error_messages:
                    if msg in reason:
                        exitcode = code
                        break
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'SUBMITTED':
                status = new
                exitcode = None
                finish_time = None
            else:
                status = inprogress
                exitcode = None
                finish_time = None

            status.append((task_id, status, exitcode, finish_time))

        return status

    def _set_server_id(self):
        result = self.mysql.query('SELECT `id` FROM `fts_servers` WHERE `url` = %s', self.server_url)
        if len(result) == 0:
            if not self.dry_run:
                self.mysql.query('INSERT INTO `fts_servers` (`url`) VALUES (%s) ON DUPLICATE KEY UPDATE `url`=VALUES(`url`)', self.server_url)
                self.server_id = self.mysql.last_insert_id
        else:
            self.server_id = result[0]
