import sys
import re
import time
import calendar
import collections
import logging
import errno

import fts3.rest.client.easy as fts3
from fts3.rest.client.request import Request

from dynamo.fileop.base import FileQuery
from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery
from dynamo.fileop.errors import find_msg_code
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Site

LOG = logging.getLogger(__name__)

class FTSFileOperation(FileTransferOperation, FileTransferQuery, FileDeletionOperation, FileDeletionQuery):
    def __init__(self, config):
        FileTransferOperation.__init__(self, config)
        FileTransferQuery.__init__(self, config)
        FileDeletionOperation.__init__(self, config)
        FileDeletionQuery.__init__(self, config)

        self.server_url = config.fts_server
        self.server_id = 0 # server id in the DB

        # Parameter "retry" for fts3.new_job. 0 = server default
        self.fts_retry = config.get('fts_retry', 0)

        # Bookkeeping device
        self.db = MySQL(config.db_params)

        # Reuse the context object
        self.keep_context = config.get('keep_context', True)
        self._context = None

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
        result = {}

        stage_files = []
        transfers = []

        s_pfn_to_task = {}
        t_pfn_to_task = {}

        for task in batch_tasks:
            sub = task.subscription
            lfn = sub.file.lfn
            dest_pfn = sub.destination.to_pfn(lfn, 'gfal2')
            source_pfn = task.source.to_pfn(lfn, 'gfal2')

            if dest_pfn is None or source_pfn is None:
                # either gfal2 is not supported or lfn could not be mapped
                result[task] = False
                continue

            if self.checksum_algorithm:
                checksum = '%s:%s' % (self.checksum_algorithm, str(sub.file.checksum[self.checksum_index]))
            else:
                checksum = None

            if task.source.storage_type == Site.TYPE_MSS:
                # need to stage first
                stage_files.append((source_pfn, dest_pfn, checksum, sub.file.size))

                # task identified by the source PFN
                s_pfn_to_task[source_pfn] = task
            else:
                transfers.append(fts3.new_transfer(source_pfn, dest_pfn, checksum = checksum, filesize = sub.file.size))

                # there should be only one task per destination pfn
                t_pfn_to_task[dest_pfn] = task

        if len(stage_files) != 0:
            job = fts3.new_staging_job([ff[0] for ff in stage_files], bring_online = 36000)
            success = self._submit_job(job, 'staging', batch_id, dict((pfn, task.id) for pfn, task in s_pfn_to_task.iteritems()))

            for source_pfn, _, _, _ in stage_files:
                result[s_pfn_to_task[source_pfn]] = success

            if success and not self._read_only:
                fields = ('id', 'source', 'destination', 'checksum', 'size')
                mapping = lambda ff: (s_pfn_to_task[ff[0]].id,) + ff
                if not self._read_only:
                    self.db.insert_many('fts_staging_queue', fields, mapping, stage_files)

        if len(transfers) != 0:
            job = fts3.new_job(transfers, retry = self.fts_retry, overwrite = False, verify_checksum = 'target')
            success = self._submit_job(job, 'transfer', batch_id, dict((pfn, task.id) for pfn, task in t_pfn_to_task.iteritems()))

            for transfer in transfers:
                dest_pfn = transfer['destinations'][0]
                result[t_pfn_to_task[dest_pfn]] = success

        return result

    def start_deletions(self, batch_id, batch_tasks): #override
        result = {}

        pfn_to_task = {}

        for task in batch_tasks:
            desub = task.desubscription
            lfn = desub.file.lfn
            pfn = desub.site.to_pfn(lfn, 'gfal2')

            if pfn is None:
                # either gfal2 is not supported or lfn could not be mapped
                result[task] = False
                continue

            # there should be only one task per destination pfn
            pfn_to_task[pfn] = task

        job = fts3.new_delete_job(pfn_to_task.keys())

        success = self._submit_job(job, 'deletion', batch_id, dict((pfn, task.id) for pfn, task in pfn_to_task.iteritems()))

        for task in pfn_to_task.itervalues():
            result[task] = success

        return result

    def cancel_transfers(self, task_ids): #override
        return self._cancel(task_ids, 'transfer')

    def cancel_deletions(self, task_ids): #override
        return self._cancel(task_ids, 'deletion')

    def cleanup(self): #override
        sql = 'DELETE FROM f USING `fts_transfer_tasks` AS f LEFT JOIN `transfer_tasks` AS t ON t.`id` = f.`id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `fts_staging_queue` AS f LEFT JOIN `fts_transfer_tasks` AS t ON t.`id` = f.`id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `fts_deletion_tasks` AS f LEFT JOIN `deletion_tasks` AS t ON t.`id` = f.`id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `fts_transfer_batches` AS f LEFT JOIN `transfer_batches` AS t ON t.`id` = f.`batch_id` WHERE t.`id` IS NULL'
        self.db.query(sql)
        sql = 'DELETE FROM f USING `fts_deletion_batches` AS f LEFT JOIN `deletion_batches` AS t ON t.`id` = f.`batch_id` WHERE t.`id` IS NULL'
        self.db.query(sql)

    def get_transfer_status(self, batch_id): #override
        if self.server_id == 0:
            self._set_server_id()

        results = []

        staged_tasks = []

        for task_id, status, exitcode, start_time, finish_time in self._get_status(batch_id, 'staging'):
            if status == FileQuery.STAT_DONE:
                staged_tasks.append(task_id)
                results.append((task_id, FileQuery.STAT_QUEUED, None, None, None))
            elif status in (FileQuery.STAT_FAILED, FileQuery.STAT_CANCELLED):
                # This is final
                results.append((task_id, status, exitcode, start_time, finish_time))

        results.extend(self._get_status(batch_id, 'transfer'))

        if len(staged_tasks) != 0:
            transfers = []
            pfn_to_tid = {}
            for task_id, source_pfn, dest_pfn, checksum, filesize in self.db.select_many('fts_staging_queue', ('id', 'source', 'destination', 'checksum', 'size'), 'id', staged_tasks):
                transfers.append(fts3.new_transfer(source_pfn, dest_pfn, checksum = checksum, filesize = filesize))
                pfn_to_tid[dest_pfn] = task_id

            job = fts3.new_job(transfers, retry = self.fts_retry, overwrite = False, verify_checksum = 'target')
            success = self._submit_job(job, 'transfer', batch_id, pfn_to_tid)
            if success and not self._read_only:
                self.db.delete_many('fts_staging_queue', 'id', pfn_to_tid.values())

        return results

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

    def _submit_job(self, job, optype, batch_id, pfn_to_tid):
        if self._read_only:
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
            if optype == 'transfer' or optype == 'staging':
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

        if optype == 'transfer' or optype == 'staging':
            table_name = 'fts_transfer_batches'
            columns = ('batch_id', 'task_type', 'fts_server_id', 'job_id')
            values = (batch_id, optype, self.server_id, job_id)
        else:
            table_name = 'fts_deletion_batches'
            columns = ('batch_id', 'fts_server_id', 'job_id')
            values = (batch_id, self.server_id, job_id)

        if not self._read_only:
            fts_batch_id = self.db.insert_get_id(table_name, columns = columns, values = values)

        if optype == 'transfer' or optype == 'staging':
            table_name = 'fts_transfer_tasks'
            pfn_key = 'dest_surl'
        else:
            table_name = 'fts_deletion_tasks'
            pfn_key = 'source_surl'

        fields = ('id', 'fts_batch_id', 'fts_file_id')
        mapping = lambda f: (pfn_to_tid[f[pfn_key]], fts_batch_id, f['file_id'])

        if not self._read_only:
            self.db.insert_many(table_name, fields, mapping, fts_files, do_update = True, update_columns = ('fts_batch_id', 'fts_file_id'))

        return True

    def _cancel(self, task_ids, optype):
        sql = 'SELECT b.`job_id`, f.`fts_file_id` FROM `fts_{op}_tasks` AS f'
        sql += ' INNER JOIN `fts_{op}_batches` AS b ON b.`id` = f.`fts_batch_id`'
        result = self.db.execute_many(sql.format(op = optype), MySQL.bare('f.`id`'), task_ids)

        by_job = collections.defaultdict(list)

        for job_id, file_id in result:
            by_job[job_id].append(file_id)
        
        for job_id, ids in by_job.iteritems():
            self._ftscall('cancel', job_id, file_ids = ids)
    
    def _get_status(self, batch_id, optype):
        if optype == 'transfer' or optype == 'staging':
            sql = 'SELECT `id`, `job_id` FROM `fts_transfer_batches`'
            sql += ' WHERE `task_type` = %s AND `fts_server_id` = %s AND `batch_id` = %s'
            batch_data = self.db.query(sql, optype, self.server_id, batch_id)
            task_table_name = 'fts_transfer_tasks'
        else:
            sql = 'SELECT `id`, `job_id` FROM `fts_deletion_batches`'
            sql += ' WHERE `fts_server_id` = %s AND `batch_id` = %s'
            batch_data = self.db.query(sql, self.server_id, batch_id)
            task_table_name = 'fts_deletion_tasks'

        message_pattern = re.compile('(?:DESTINATION|SOURCE|TRANSFER) \[([0-9]+)\] (.*)')

        results = []

        for fts_batch_id, job_id in batch_data:
            sql = 'SELECT `fts_file_id`, `id` FROM `{table}` WHERE `fts_batch_id` = %s'.format(table = task_table_name)
            fts_to_task = dict(self.db.xquery(sql, fts_batch_id))

            result = self._ftscall('get_job_status', job_id = job_id, list_files = True)
    
            if optype == 'transfer' or optype == 'staging':
                fts_files = result['files']
            else:
                fts_files = result['dm']

            for fts_file in fts_files:
                try:
                    task_id = fts_to_task[fts_file['file_id']]
                except KeyError:
                    continue
    
                state = fts_file['file_state']
                exitcode = None
                start_time = None
                finish_time = None
                get_time = False

                try:
                    message = fts_file['reason']
                    # Check if reason follows a known format (from which we can get the exit code)
                    matches = message_pattern.match(message)
                    if matches is not None:
                        exitcode = int(matches.group(1))
                        message = matches.group(2)
                    # Additionally, if the message is a known one, convert the exit code
                    c = find_msg_code(message)
                    if c is not None:
                        exitcode = c

                except KeyError:
                    message = None
    
                if state == 'FINISHED':
                    status = FileQuery.STAT_DONE
                    exitcode = 0
                    get_time = True

                elif state == 'FAILED':
                    status = FileQuery.STAT_FAILED
                    get_time = True

                elif state == 'CANCELED':
                    status = FileQuery.STAT_CANCELLED
                    exitcode = -1
                    get_time = True

                elif state == 'SUBMITTED':
                    status = FileQuery.STAT_NEW

                else:
                    status = FileQuery.STAT_QUEUED

                if optype == 'transfer' and exitcode == errno.EEXIST:
                    # Transfer + destination exists -> not an error
                    status = FileQuery.STAT_DONE
                    exitcode = 0
                elif optype == 'deletion' and exitcode == errno.ENOENT:
                    # Deletion + destination does not exist -> not an error
                    status = FileQuery.STAT_DONE
                    exitcode = 0
                    
                if get_time:
                    start_time = calendar.timegm(time.strptime(fts_file['start_time'], '%Y-%m-%dT%H:%M:%S'))
                    finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
    
                results.append((task_id, status, exitcode, message, start_time, finish_time))

        return results

    def _forget_status(self, task_id, optype):
        if self._read_only:
            return

        sql = 'DELETE FROM `fts_{optype}_tasks` WHERE `id` = %s'.format(optype = optype)
        self.db.query(sql, task_id)

    def _forget_batch(self, batch_id, optype):
        if self._read_only:
            return

        sql = 'DELETE FROM `fts_{optype}_batches` WHERE `batch_id` = %s'.format(optype = optype)
        self.db.query(sql, batch_id)

    def _set_server_id(self):
        result = self.db.query('SELECT `id` FROM `fts_servers` WHERE `url` = %s', self.server_url)
        if len(result) == 0:
            if not self._read_only:
                self.db.query('INSERT INTO `fts_servers` (`url`) VALUES (%s) ON DUPLICATE KEY UPDATE `url`=VALUES(`url`)', self.server_url)
                self.server_id = self.db.last_insert_id
        else:
            self.server_id = result[0]
