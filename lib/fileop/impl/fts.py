import sys
import time
import calendar
import collections
import logging

import fts3.rest.client.easy as fts3
from fts3.rest.client.request import Request

from dynamo.fileop.base import FileQuery
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
        self.db = MySQL(config.db_params)

        # Cache the error messages
        self.error_messages = self.db.query('SELECT `code`, `message` FROM `fts_error_messages`')

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

        return self._submit_job(job, 'transfer', batch_id, pfn_to_task)

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

    def cancel_transfers(self, task_ids): #override
        return self._cancel(task_ids, 'transfer')

    def cancel_deletions(self, task_ids): #override
        return self._cancel(task_ids, 'deletion')

    def get_transfer_status(self, batch_id): #override
        if self.server_id == 0:
            self._set_server_id()

        return self._get_status(batch_id, 'transfer')

    def get_deletion_status(self, batch_id): #override
        if self.server_id == 0:
            self._set_server_id()

        return self._get_status(batch_id, 'deletion')

    def forget_transfer_status(self, task_id): #override
        return self._forget_status(task_id, 'transfer')

    def forget_deletion_status(self, task_id): #override
        return self._forget_status(task_id, 'deletion')

    def forget_transfer_batch(self, task_id): #override
        return self._forget_batch(task_id, 'transfer')

    def forget_deletion_batch(self, task_id): #override
        return self._forget_batch(task_id, 'deletion')

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

        # lock against cancellation
        self.db.lock_tables(write = ['fts_' + optype + '_batches', 'fts_' + optype + '_files'])
        try:
            sql = 'INSERT INTO `fts_{op}_batches` (`batch_id`, `fts_server_id`, `job_id`)'.format(op = optype)
            sql += ' VALUES (%s, %s, %s)'
            if not self.dry_run:
                self.db.query(sql, batch_id, self.server_id, job_id)
    
            fields = (optype + '_id', 'batch_id', 'fts_file_id')
            mapping = lambda f: (pfn_to_task[f['dest_surl']].id, batch_id, f['file_id'])
    
            if not self.dry_run:
                self.db.insert_many('fts_' + optype + '_files', fields, mapping, fts_files)
        finally:
            self.db.unlock_tables()

        return True

    def _cancel(self, task_ids, optype):
        sql = 'SELECT b.`job_id`, f.`fts_file_id` FROM `fts_{op}_files` AS f'
        sql += ' INNER JOIN `fts_{op}_batches` AS b ON b.`batch_id` = f.`batch_id`'
        result = self.db.execute_many(sql.format(op = optype), MySQL.bare('f.`{op}_id`'.format(op = optype)), task_ids)

        by_job = collections.defaultdict(list)

        for job_id, file_id in result:
            by_job[job_id].append(file_id)
        
        for job_id, ids in by_job.iteritems():
            self._ftscall('cancel', job_id, file_ids = ids)
    
    def _get_status(self, batch_id, optype):
        sql = 'SELECT `job_id` FROM `fts_{optype}_batches`'
        sql += ' WHERE `fts_server_id` = %s AND `batch_id` = %s'

        job_id = self.db.query(sql.format(optype = optype), self.server_id, batch_id)[0]

        sql = 'SELECT `fts_file_id`, `{optype}_id` FROM `fts_{optype}_files` WHERE `batch_id` = %s'
        fts_to_queue = dict(self.db.xquery(sql.format(optype = optype), batch_id))

        result = self._ftscall('get_job_status', job_id = job_id, list_files = True)

        if optype == 'transfer':
            fts_files = result['files']
        else:
            fts_files = result['dm']

        results = []

        for fts_file in fts_files:
            try:
                task_id = fts_to_queue[fts_file['file_id']]
            except KeyError:
                continue

            state = fts_file['file_state']

            if state == 'FINISHED':
                status = FileQuery.STAT_DONE
                exitcode = 0
                start_time = calendar.timegm(time.strptime(fts_file['start_time'], '%Y-%m-%dT%H:%M:%S'))
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'FAILED':
                status = FileQuery.STAT_FAILED
                exitcode = -1
                for code, msg in self.error_messages:
                    if msg in reason:
                        exitcode = code
                        break
                start_time = calendar.timegm(time.strptime(fts_file['start_time'], '%Y-%m-%dT%H:%M:%S'))
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'CANCELED':
                status = FileQuery.STAT_CANCELLED
                exitcode = -1
                start_time = calendar.timegm(time.strptime(fts_file['start_time'], '%Y-%m-%dT%H:%M:%S'))
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'SUBMITTED':
                status = FileQuery.STAT_NEW
                exitcode = None
                start_time = None
                finish_time = None
            else:
                status = FileQuery.STAT_INPROGRESS
                exitcode = None
                start_time = None
                finish_time = None

            results.append((task_id, status, exitcode, start_time, finish_time))

        return results

    def _forget_status(self, task_id, optype):
        if self.dry_run:
            return

        # Function may be called under table locks. Need to lock our tables
        self.db.lock_tables(write = ['fts_' + optype + '_files'])

        try:
            sql = 'DELETE FROM `fts_{optype}_files` WHERE `{optype}_id` = %s'.format(optype = optype)
            self.db.query(sql, task_id)
        finally:
            self.db.unlock_tables()

    def _forget_batch(self, batch_id, optype):
        if self.dry_run:
            return

        # Function may be called under table locks. Need to lock our tables
        self.db.lock_tables(write = ['fts_' + optype + '_batches'])

        try:
            sql = 'DELETE FROM `fts_{optype}_batches` WHERE `batch_id` = %s'.format(optype = optype)
            self.db.query(sql, batch_id)
        finally:
            self.db.unlock_tables()

    def _set_server_id(self):
        result = self.db.query('SELECT `id` FROM `fts_servers` WHERE `url` = %s', self.server_url)
        if len(result) == 0:
            if not self.dry_run:
                self.db.query('INSERT INTO `fts_servers` (`url`) VALUES (%s) ON DUPLICATE KEY UPDATE `url`=VALUES(`url`)', self.server_url)
                self.server_id = self.db.last_insert_id
        else:
            self.server_id = result[0]
