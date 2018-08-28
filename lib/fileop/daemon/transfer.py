import errno
import gfal2

from dynamo.fileop.daemon.manager import PoolManager, StatefulPoolManager
from dynamo.fileop.gfal_exec import gfal_exec

transfer_nonerrors = {
    errno.EEXIST: 'Destination file exists.' # for a transfer task, 17 means that file exists at the destination
                                             # - should check file size and checksum with context.stat(dest_pfn)
}

class TransferPoolManager(StatefulPoolManager):
    def __init__(self, src, dest, max_concurrent, proxy):
        name = '%s-%s' % (src, dest)
        opformat = '{0} -> {1}'
        PoolManager.__init__(self, name, 'transfer', opformat, max_concurrent, proxy)

    def task(self, task_id, src_pfn, dest_pfn, params_config):
        """
        Transfer task worker process
        @param task_id         Task id in the queue.
        @param src_pfn         Source PFN
        @param dest_pfn        Destination PFN
        @param params_config   Configuration parameters used to create GFAL2 transfer parameters.
    
        @return  (exit code, start time, finish time, error message, log string)
        """

        activated = PoolManager.db.query('UPDATE `standalone_transfer_tasks` SET `status` = \'active\' WHERE `id` = %s', task_id)    
        if activated == 0:
            # task was cancelled
            return -1, None, None, '', ''
    
        if not params_config['overwrite']:
            # At least for some sites, transfers with overwrite = False still overwrites the file. Try stat first
            stat_result = gfal_exec('stat', (dest_pfn,))
    
            if stat_result[0] == 0:
                return stat_result
    
        try:
            params = gfal2.Gfal2Context.transfer_parameters()
            # Create parent directories at the destination
            params.create_parent = True
            # Overwrite the destination if file already exists (otherwise throws an error)
            params.overwrite = params_config['overwrite']
            if 'checksum' in params_config:
                params.set_checksum(*params_config['checksum'])
            params.timeout = params_config['transfer_timeout'] # we probably want this to be file size dependent
        
        except Exception as exc:
            # multiprocessing pool cannot handle certain exceptions - convert to string
            raise Exception(str(exc))
    
        return gfal_exec('filecopy', (params, src_pfn, dest_pfn), transfer_nonerrors)

