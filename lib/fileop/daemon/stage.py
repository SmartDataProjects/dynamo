import logging

from dynamo.fileop.daemon.manager import PoolManager
from dynamo.fileop.daemon.gfal_exec import gfal_exec

LOG = logging.getLogger(__name__)

class StagingPoolManager(PoolManager):
    def __init__(self, site, max_concurrent, proxy):
        opformat = '{0}'
        PoolManager.__init__(self, site, 'staging', opformat, max_concurrent, proxy)

    def process_result(self, result_tuple):
        delim = '--------------'

        tid, result = result_tuple[:2]
        args = result_tuple[2:]

        staged = result.get()

        opstring = self.opformat.format(*args)

        if not staged:
            return

        LOG.info('%s: staged %s', self.name, opstring)

        sql = 'UPDATE `standalone_transfer_tasks` SET `status` = \'staged\' WHERE `id` = %s'

        PoolManager.db.query(sql, tid)

    def task(self, task_id, pfn, token):
        """
        Staging task worker process
        @param task_id        Task id in the queue.
        @param pfn            PFN
        @param token          Gfal2 staging token
    
        @return  boolean (True if staged)
        """
    
        status = gfal_exec('bring_online_poll', (pfn, token), return_value = True)

        return status == 1
