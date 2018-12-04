import os
import threading
import signal
import multiprocessing
import logging

LOG = logging.getLogger(__name__)

class PoolManager(object):
    """
    Base class for managing one task pool. Asynchronous results of the tasks are collected
    in collect_results() running as a separate thread, automatically started when the first
    task is added to the pool
    """

    db = None
    stop_flag = None
    ## Need to have a global signal converter that subprocesses can unset blocking
    signal_converter = None

    def __init__(self, name, optype, opformat, task, max_concurrent, proxy):
        """
        @param name           Name of the instance. Used in logging.
        @param optype         'transfer' or 'deletion'.
        @param opformat       Format string used in logging.
        @param task           Task function to run
        @param max_concurrent Maximum number of concurrent processes in the pool.
        @param proxy          X509 proxy
        """

        self.name = name
        self.optype = optype
        self.task = task
        self.opformat = opformat
        self.proxy = proxy

        self._pool = multiprocessing.Pool(max_concurrent, initializer = self._pre_exec)
        self._results = []
        self._collector_thread = None
        self._closed = False

    def add_task(self, tid, *args):
        """
        Add a task to the pool and start the results collector.
        """

        if self._closed:
            raise RuntimeError('PoolManager %s is closed' % self.name)

        if not self._set_queued(tid):
            return

        opstring = self.opformat.format(*args)
        LOG.info('%s: %s %s', self.name, self.optype, opstring)

        proc_args = (tid,) + args
        async_result = self._pool.apply_async(self.task, proc_args)
        self._results.append((tid, async_result) + args)

        if self._collector_thread is None or not self._collector_thread.is_alive():
            self.start_collector()

    def process_result(self, result_tuple):
        """
        Process the result of a completed task.
        """
        pass

    def ready_for_recycle(self):
        """
        Check if this pool manager can be shut down. Managers should be shut down whenever
        possible to keep the resource (threads and subprocesses) usage down and also to
        adjust the concurrency on each link as needed.
        """

        if self._closed:
            return True

        if len(self._results) != 0:
            return False

        if self._collector_thread is None:
            return True

        if self._collector_thread.is_alive():
            return False

        if PoolManager.stop_flag.is_set():
            LOG.warning('Terminating pool %s' % self.name)
            self._pool.terminate()

        self._pool.close()
        self._pool.join()

        self._collector_thread.join()

        self._closed = True

        return True

    def start_collector(self):
        if self._collector_thread is not None:
            self._collector_thread.join()

        self._collector_thread = threading.Thread(target = self.collect_results, name = self.name)
        self._collector_thread.start()

    def collect_results(self):
        while len(self._results) != 0:
            ir = 0
            while ir != len(self._results):
                if PoolManager.stop_flag.is_set():
                    return
    
                if not self._results[ir][1].ready():
                    ir += 1
                    continue
    
                self.process_result(self._results.pop(ir))
    
            is_set = PoolManager.stop_flag.wait(5)
            if is_set: # True if Python 2.7 + flag is set
                return

    def _pre_exec(self):
        PoolManager.signal_converter.unset(signal.SIGTERM)
        PoolManager.signal_converter.unset(signal.SIGHUP)
        
        if self.proxy:
            os.environ['X509_USER_PROXY'] = self.proxy

    def _set_queued(self, task_id):
        return True


class StatefulPoolManager(PoolManager):
    """
    PoolManager for tasks with states (transfer and deletion).
    """

    def process_result(self, result_tuple):
        """
        Process the result of a completed task.
        """

        delim = '--------------'

        tid, result = result_tuple[:2]
        args = result_tuple[2:]

        exitcode, start_time, finish_time, msg, log = result.get()

        if finish_time is not None and start_time is not None:
            optime = finish_time - start_time
        else:
            optime = '-'
        opstring = self.opformat.format(*args)

        if exitcode == -1:
            LOG.info('%s: cancelled %s %s', self.name, self.optype, opstring)
            status = 'cancelled'
        elif exitcode == 0:
            LOG.info('%s: succeeded %s (%s s) %s\n%s\n%s%s', self.name, self.optype, optime, opstring, delim, log, delim)
            status = 'done'
        else:
            LOG.info('%s: failed %s (%s s, %d: %s) %s\n%s\n%s%s', self.name, self.optype, optime, exitcode, msg, opstring, delim, log, delim)
            status = 'failed'

        sql = 'UPDATE `standalone_{op}_tasks` SET `status` = %s, `exitcode` = %s, `message` = %s, `start_time` = FROM_UNIXTIME(%s), `finish_time` = FROM_UNIXTIME(%s) WHERE `id` = %s'.format(op = self.optype)

        PoolManager.db.query(sql, status, exitcode, msg, start_time, finish_time, tid)

    def _set_queued(self, task_id):
        sql = 'UPDATE `standalone_{op}_tasks` SET `status` = \'queued\' WHERE `id` = %s'.format(op = self.optype)
        updated = PoolManager.db.query(sql, tid)
        return updated != 0
