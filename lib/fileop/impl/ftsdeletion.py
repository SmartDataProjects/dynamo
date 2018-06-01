import fts3.rest.client.easy as fts3

from dynamo.fileop.deletion import FileDeletionOperation, FileDeletionQuery
from dynamo.fileop.impl.fts import FTSInterface

class FTSFileDeletion(FileDeletionOperation, FileDeletionQuery):
    def __init__(self, config):
        FileDeletionOperation.__init__(self, config)
        FileDeletionQuery.__init__(self, config)
        FTSInterface.__init__(self, config)

    def form_batches(self, tasks):
        return FTSInterface.form_batches(self, tasks)

    def start_deletions(self, batch_id, batch_tasks):
        pfn_to_task = {}

        for task in batch_tasks:
            desub = task.desubscription
            lfn = desub.file.lfn
            pfn = desub.site.to_pfn(lfn, 'gfal2')

            deletions.append(fts3.new_deletion(source_pfn, dest_pfn))

            # there should be only one task per destination pfn
            pfn_to_task[dest_pfn] = task

        # verify = False -> do not verify the server certificate
        context = fts3.Context(self.fts_server, verify = False)

        job = fts3.new_delete_job(pfn_to_task.keys())

        job_id = fts3.submit(context, job)

        sql = 'INSERT INTO `fts_deletion_batches` (`batch_id`, `fts_server_id`, `job_id`)'
        sql += ' SELECT %s, `id`, %s FROM `fts_servers` WHERE `url` = %s'
        self.mysql.query(sql, batch_id, job_id, self.fts_server)

        # list of file-level deletions
        fts_dm = fts3.get_job_status(context, job_id = job_id, list_files = True)['dm']

        fields = ('deletion_id', 'batch_id', 'fts_file_id')
        mapping = lambda f: (pfn_to_task[f['dest_surl']].id, batch_id, f['file_id'])

        self.mysql.insert_many('fts_deletion_files', fields, mapping, fts_files)

    def get_status(self, batch_id):
        return FTSInterface.get_status(self, batch_id, 'deletion')
