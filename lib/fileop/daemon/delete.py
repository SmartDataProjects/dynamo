import stat
import errno

from dynamo.fileop.daemon.manager import PoolManager, StatefulPoolManager
from dynamo.fileop.daemon.gfal_exec import gfal_exec

deletion_nonerrors = {
    errno.ENOENT: 'Target file does not exist.'
}

class DeletionPoolManager(StatefulPoolManager):
    def __init__(self, site, max_concurrent, proxy):
        opformat = '{0}'
        PoolManager.__init__(self, site, 'deletion', opformat, DeletionPoolManager.task, max_concurrent, proxy)

    @staticmethod
    def task(task_id, pfn):
        """
        Deletion task worker process
        @param task_id        Task id in the queue.
        @param pfn            Target PFN
    
        @return  (exit code, start time, finish time, error message, log string)
        """

        activated = PoolManager.db.query('UPDATE `standalone_deletion_tasks` SET `status` = \'active\' WHERE `id` = %s', task_id)
        if activated == 0:
            # task was cancelled
            return -1, None, None, '', ''            
    
        return gfal_exec('unlink', (pfn,), deletion_nonerrors)


class UnmanagedDeletionPoolManager(PoolManager):
    def __init__(self, site, max_concurrent, proxy):
        opformat = '{0}'
        PoolManager.__init__(self, site, 'unmanaged_deletion', opformat, UnmanagedDeletionPoolManager.task, max_concurrent, proxy)

    @staticmethod
    def task(task_id, url):
        """
        Deletion task worker process
        @param task_id        Task id in the queue.
        @param url            Target URL (file or directory name)
    
        @return  (0, start time, finish time, error message, log string)
                 note: all results are considered success
        """

        PoolManager.db.query('DELETE FROM `unmanaged_deletions` WHERE `id` = %s', task_id)

        try:
            stat_result = gfal_exec('stat', (url,), return_value = True)
        except:
            return 0, None, None, 'stat error', ''

        if stat.S_ISDIR(stat_result.st_mode):
            # this is a directory
            result = gfal_exec('rmdir', (url,))
        else:
            result = gfal_exec('unlink', (url,))

        return (0,) + rmdir_result[1:]
