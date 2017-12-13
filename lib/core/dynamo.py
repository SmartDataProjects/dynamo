import os
import sys
import signal
import time
import logging
import hashlib
import multiprocessing
import Queue

from core.inventory import DynamoInventory
from core.registry import DynamoRegistry
from common.control import SignalBlocker
import core.executable

LOG = logging.getLogger(__name__)

class Dynamo(object):
    """Main daemon class."""

    CMD_UPDATE, CMD_DELETE = range(2)

    def __init__(self, config):
        LOG.info('Initializing Dynamo server.')

        ## Create the registry
        self.registry = DynamoRegistry(config.registry)
        self.registry_config = config.registry.clone()

        ## Create the inventory
        self.inventory = DynamoInventory(config.inventory, load = False)
        self.inventory_config = config.inventory.clone()

        ## Load the inventory content (filter according to debug config)
        load_opts = {}
        for objs in ['groups', 'sites', 'datasets']:
            try:
                included = config.debug['included_' + objs]
            except KeyError:
                included = None
            try:
                excluded = config.debug['excluded_' + objs]
            except KeyError:
                excluded = None

            load_opts[objs] = (included, excluded)
        
        LOG.info('Loading the inventory.')
        self.inventory.load(**load_opts)

        # configuration containing privileged-access passwords
        self.restricted_config = config.restricted_config.clone()

    def run(self):
        """
        Infinite-loop main body of the daemon.
        Step 1: Poll the registry for one uploaded script.
        Step 2: If a script is found, check the authorization of the script.
        Step 3: Spawn a child process for the script with either a partition or the inventory as an argument.
        Step 4: Collect completed child processes.
        Step 5: Sleep for N seconds.
        """

        LOG.info('Started dynamo daemon.')

        child_processes = []

        # There can only be one child process with write access at a time.
        writing = False

        signal_blocker = SignalBlocker(logger = LOG)

        LOG.info('Start polling for executables.')

        try:
            first_wait = True
            sleep_time = 0
            while True:
                time.sleep(sleep_time)

                LOG.debug('Polling for executables.')
    
                sql = 'SELECT s.`id`, s.`write_request`, s.`title`, s.`path`, s.`args`, s.`user_id`, u.`name`'
                sql += ' FROM `action` AS s INNER JOIN `users` AS u ON u.`id` = s.`user_id`'
                sql += ' WHERE s.`status` = \'new\''
                if writing:
                    # we don't allow write_requesting executables while there is one running
                    sql += ' AND s.`write_request` = 0'
                sql += ' ORDER BY s.`timestamp` LIMIT 1'
                result = self.registry.backend.query(sql)

                if len(result) == 0:
                    if len(child_processes) == 0 and first_wait:
                        LOG.info('Waiting for executables.')
                        first_wait = False

                    sleep_time = 0.5

                    LOG.debug('No executable found, sleeping for %d seconds.', sleep_time)

                else:
                    exec_id, write_request, title, path, args, user_id, user_name = result[0]

                    first_wait = True
                    sleep_time = 0

                    if not os.path.exists(path + '/exec.py'):
                        LOG.info('Executable %s from user %s (write request: %s) not found.', title, user_name, write_request)
                        self.registry.backend.query('UPDATE `action` SET `status` = %s WHERE `id` = %s', 'failed', exec_id)
                        continue

                    LOG.info('Found executable %s from user %s (write request: %s)', title, user_name, write_request)

                    if write_request:
                        if not self.check_write_auth(title, user_id, path):
                            LOG.warning('Executable %s from user %s is not authorized for write access.', title, user_name)
                            # send a message

                            self.registry.backend.query('UPDATE `action` SET `status` = %s where `id` = %s', 'failed', exec_id)
                            continue
    
                        queue = multiprocessing.Queue()

                        writing = True

                    else:
                        queue = None

                    self.registry.backend.query('UPDATE `action` SET `status` = %s WHERE `id` = %s', 'run', exec_id)
    
                    proc = multiprocessing.Process(target = self._run_one, name = title, args = (path, args, queue))
                    child_processes.append((exec_id, proc, user_name, path, queue))
    
                    proc.daemon = True
                    proc.start()
    
                    LOG.info('Started executable %s (%s) from user %s (PID %d).', title, path, user_name, proc.pid)


                completed_processes = self.collect_processes(child_processes)

                for (_, _, _, _, queue), status in completed_processes:
                    if queue is not None:
                        # This was a write-enabled process and it completed

                        writing = False

                        if status == 'done':
                            # The child process may send us the list of updated/deleted objects
                            # Block system signals and get update done
                            signal_blocker.block(signal.SIGINT)
                            signal_blocker.block(signal.SIGTERM)
                            while True:
                                try:
                                    cmd, obj = queue.get()
                                except Queue.Empty:
                                    break
                                else:
                                    if cmd == Dynamo.CMD_UPDATE:
                                        self.inventory.update(obj, write = True)
                                    elif cmd == Dynamo.CMD_DELETE:
                                        self.inventory.delete(obj, write = True)
    
                            signal_blocker.unblock(signal.SIGINT)
                            signal_blocker.unblock(signal.SIGTERM)

        except KeyboardInterrupt:
            LOG.info('Main process was interrupted.')

        except:
            # log the exception
            LOG.warning('Exception in main process. Terminating all child processes.')
            raise

        finally:
            # If the main process was interrupted by Ctrl+C:
            # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
            # foreground process group). In this case calling terminate() will duplicate signals
            # in the child. Child processes have to always ignore SIGINT and be killed only from
            # SIGTERM sent by the line below.

            for exec_id, proc, user_name, path, queue in child_processes:
                LOG.warning('Terminating %s (%s) requested by %s (PID %d)', proc.name, path, user_name, proc.pid)
                proc.terminate()
                proc.join(5)
                if proc.is_alive():
                    LOG.warning('Child process %d did not return after 5 seconds.', proc.pid)

                if queue is not None:
                    queue.close()

                self.registry.backend.query('UPDATE `action` SET `status` = \'killed\' where `id` = %s', exec_id)

    def check_write_auth(self, title, user_id, path):
        # check authorization
        with open(path + '/exec.py') as source:
            checksum = hashlib.md5(source.read()).hexdigest()

        sql = 'SELECT `user_id` FROM `authorized_executables` WHERE `title` = %s AND `checksum` = UNHEX(%s)'
        for auth_user_id in self.registry.backend.query(sql, title, checksum):
            if auth_user_id == 0 or auth_user_id == user_id:
                return True

        return False

    def collect_processes(self, child_processes):
        completed_processes = []

        ichild = 0
        while ichild != len(child_processes):
            exec_id, proc, user_name, path, _ = child_processes[ichild]

            status = 'done'

            result = self.registry.backend.query('SELECT `status` FROM `action` WHERE `id` = %s', exec_id)
            if len(result) == 0 or result[0] != 'run':
                # Job was aborted in the registry
                proc.terminate()
                proc.join(5)
                status = 'killed'
    
            if proc.is_alive():
                ichild += 1
            else:
                if status == 'done' and proc.exitcode != 0:
                    status = 'failed'

                LOG.info('Executable %s (%s) from user %s completed (Exit code %d Status %s).', proc.name, path, user_name, proc.exitcode, status)

                child_proc = child_processes.pop(ichild)
                completed_processes.append((child_proc, status))

                self.registry.backend.query('UPDATE `action` SET `status` = %s where `id` = %s', status, exec_id)

        return completed_processes
        
    def _run_one(self, path, args, queue):
        ## Ignore SIGINT - see note above proc.terminate()
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        ## We will react to SIGTERM by raising KeyboardInterrupt
        from common.control import SignalConverter
        signal_converter = SignalConverter()
        signal_converter.set(signal.SIGTERM)

        # Redirect STDOUT and STDERR to file, close STDIN
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = open(path + '/_stdout', 'a')
        sys.stderr = open(path + '/_stderr', 'a')
        sys.stdin.close()

        # Set argv
        sys.argv = [path + '/exec.py'] + args.split()

        # Restart logging
        logging.shutdown()
        reload(logging)

        # common.interface is loaded by registry - logging still points to the main process
        import common.interface
        for name in dir(common.interface):
            mod = getattr(common.interface, name)
            if type(mod).__name__ == 'module':
                reload(mod)

        # Re-initialize
        #  - inventory store with read-only connection
        #  - registry backend with read-only connection
        # This is for security and simply for concurrency - multiple processes
        # should not share the same DB connection
        backend_config = self.registry_config.backend
        self.registry.set_backend(backend_config.module, backend_config.readonly_config)

        persistency_config = self.inventory_config.persistency
        self.inventory.init_store(persistency_config.module, persistency_config.readonly_config)

        # Pass my registry and inventory to the executable through core.executable
        core.executable.registry = self.registry
        core.executable.inventory = self.inventory

        if queue is not None:
            # create a list of updated objects the executable can fill
            core.executable.inventory._updated_objects = []
            core.executable.inventory._deleted_objects = []

            core.executable.interface_configs = self.restricted_config

        execfile(path + '/exec.py')

        if queue is not None:
            for obj in self.inventory._updated_objects:
                queue.put((Dynamo.CMD_UPDATE, obj))
            for obj in self.inventory._deleted_objects:
                queue.put((Dynamo.CMD_DELETE, obj))

            # can we close and quit here?
            while not queue.empty():
                time.sleep(1)

        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = stdout
        sys.stderr = stderr
        
