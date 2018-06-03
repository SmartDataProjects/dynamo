import sys
import logging

import fts3.rest.client.easy as fts3

from dynamo.fileop.transfer import FileTransferOperation, FileTransferQuery
from dynamo.fileop.impl.fts import FTSInterface

LOG = logging.getLogger(__name__)

class FTSFileTransfer(FileTransferOperation, FileTransferQuery, FTSInterface):
    def __init__(self, config):
        FileTransferOperation.__init__(self, config)
        FileTransferQuery.__init__(self, config)
        FTSInterface.__init__(self, config)

    def form_batches(self, tasks): #override
        return FTSInterface.form_batches(self, tasks)

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

        if self.fts_server_id == 0:
            self.set_server_id()

        job = fts3.new_job(transfers, retry = self.fts_retry, overwrite = True, verify_checksum = False)

        if self.dry_run:
            job_id = 'test'
        else:
            try:
                job_id = self.ftscall('submit', job)
            except:
                exc_type, exc, tb = sys.exc_info()
                LOG.error('Failed to submit transfer to FTS: Exception %s (%s)', exc_type.__name__, str(exc))
                return False

        # list of file-level transfers (one-to-one with fts3.new_transfer)
        try:
            fts_files = self.ftscall('get_job_status', job_id = job_id, list_files = True)['files']
        except:
            exc_type, exc, tb = sys.exc_info()
            LOG.error('Failed to get status of job %s from FTS: Exception %s (%s)', job_id, exc_type.__name__, str(exc))
            return False

        sql = 'INSERT INTO `fts_transfer_batches` (`batch_id`, `fts_server_id`, `job_id`)'
        sql += ' VALUES (%s, %s, %s)'
        if not self.dry_run:
            self.mysql.query(sql, batch_id, self.fts_server_id, job_id)

        fields = ('transfer_id', 'batch_id', 'fts_file_id')
        mapping = lambda f: (pfn_to_task[f['dest_surl']].id, batch_id, f['file_id'])

        if not self.dry_run:
            self.mysql.insert_many('fts_transfer_files', fields, mapping, fts_files)

        return True

    def get_status(self, batch_id): #override
        return FTSInterface.get_status(self, batch_id, 'transfer')
