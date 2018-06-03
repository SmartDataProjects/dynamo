import time
import calendar

import fts3.rest.client.easy as fts3
from fts3.rest.client.request import Request
from dynamo.utils.interface.mysql import MySQL
from dynamo.fileop.transfer import FileTransferQuery
from dynamo.fileop.deletion import FileDeletionQuery

class FTSInterface(object):
    """
    Common methods for FTS Transfer and Deletion utilities. Using MySQL for bookkeeping.
    """

    def __init__(self, config):
        self.fts_server = config.fts_server
        self.fts_server_id = 0 # server id in the DB

        self.mysql = MySQL(config.db_params)

        # Parameter "retry" for fts3.new_job
        self.fts_retry = config.fts_retry

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

    def get_exit_code(self, reason):
        for code, msg in self.error_messages:
            if msg in reason:
                return code
    
        return -1

    def ftscall(self, method, *args, **kwd):
        if self._context is None:
            # request_class = Request -> use "requests"-based https call (instead of default PyCURL, which may not be able to handle proxy certificates depending on the cURL installation)
            # verify = False -> do not verify the server certificate
            context = fts3.Context(self.fts_server, request_class = Request, verify = False)

            if self.keep_context:
                self._context = context
        else:
            context = self._context

        return getattr(fts3, method)(context, *args, **kwd)

    def get_status(self, batch_id, optype):
        if self.fts_server_id == 0:
            self.set_server_id()

        sql = 'SELECT `job_id` FROM `fts_{optype}_batches`'
        sql += ' WHERE `fts_server_id` = %s, `batch_id` = %s'

        job_id = self.mysql.query(sql.format(optype = optype), self.fts_server_id, batch_id)[0]

        sql = 'SELECT `fts_file_id`, `{optype}_id` FROM `fts_{optype}_files` WHERE `batch_id` = %s'
        fts_to_queue = dict(self.mysql.xquery(sql.format(optype = optype), batch_id))

        result = self.ftscall('get_job_status', job_id = job_id, list_files = True)

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
                transfer_id = fts_to_queue[fts_file['file_id']]
            except KeyError:
                continue

            state = fts_file['file_state']

            if state == 'FINISHED':
                status = done
                exitcode = 0
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'FAILED':
                status = failed
                exitcode = self.get_exit_code(fts_file['reason'])
                finish_time = calendar.timegm(time.strptime(fts_file['finish_time'], '%Y-%m-%dT%H:%M:%S'))
            elif state == 'SUBMITTED':
                status = new
                exitcode = None
                finish_time = None
            else:
                status = inprogress
                exitcode = None
                finish_time = None

            status.append((transfer_id, status, exitcode, finish_time))

        return status

    def set_server_id(self):
        result = self.mysql.query('SELECT `id` FROM `fts_servers` WHERE `url` = %s', self.fts_server)
        if len(result) == 0:
            if not self.dry_run:
                self.mysql.query('INSERT INTO `fts_servers` (`url`) VALUES (%s) ON DUPLICATE KEY UPDATE `url`=VALUES(`url`)', self.fts_server)
                self.fts_server_id = self.mysql.last_insert_id
        else:
            self.fts_server_id = result[0]
