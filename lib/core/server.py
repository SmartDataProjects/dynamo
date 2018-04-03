import os
import sys
import pwd
import time
import logging
import hashlib
import shlex
import signal
import socket
import multiprocessing
import threading
import Queue

from dynamo.core.inventory import DynamoInventory
from dynamo.core.manager.base import ServerManager
from dynamo.utils.signaling import SignalBlocker
import dynamo.utils.interface as interface
import dynamo.core.manager.impl as manager_impl
from dynamo.core.manager.base import OutOfSyncError
from dynamo.dataformat.exceptions import log_exception

LOG = logging.getLogger(__name__)
CHANGELOG = logging.getLogger('changelog')

def killproc(proc):
    uid = os.geteuid()
    os.seteuid(0)
    proc.terminate()
    os.seteuid(uid)
    proc.join(5)

class Dynamo(object):
    """Main daemon class."""

    def __init__(self, config):
        LOG.info('Initializing Dynamo server %s.', __file__)

        ## User names
        # User with full privilege (still not allowed to write to inventory store)
        self.full_user = config.user
        # Restricted user
        self.read_user = config.read_user

        ## Create the inventory
        self.inventory = DynamoInventory(config.inventory, load = False)
        self.inventory_config = config.inventory.clone()

        ## Create the server manager
        manager_config = config.manager.clone()
        manager_config['has_store'] = ('persistency' in self.inventory_config)
        self.manager = DynamoServerManager(manager_config)

        if self.manager.has_store:
            self.manager.advertise_store(self.inventory_config.persistency)

        ## Load the inventory content (filter according to debug config)
        self.inventory_load_opts = {}
        if 'debug' in config:
            for objs in ['groups', 'sites', 'datasets']:
                included = config.debug.get('included_' + objs, None)
                excluded = config.debug.get('excluded_' + objs, None)
    
                self.inventory_load_opts[objs] = (included, excluded)

    def load_inventory(self):
        self.manager.set_status(ServerManager.SRV_STARTING)

        ## Wait until there is no write process
        while True:
            writing_process_id = self.manager.writing_process_id()
            if write_process_id is not None:
                LOG.debug('A write-enabled process is running. Checking again in 5 seconds.')
                time.sleep(5)
            else:
                break

        ## Write process is done.
        ## Other servers will not start a new write process while there is a server with status 'starting'.
        ## The only states the other running servers can be in are therefore 'updating' or 'online'

        ## Now find a server I'll load inventory from (unless I am the only online host and I have a store)
        if not (self.manager.has_store and self.manager.count_servers(ServerManager.SRV_ONLINE) == 0):
            self.setup_remote_store()

        LOG.info('Loading the inventory.')
        self.inventory.load(**self.inventory_load_opts)

        if self.manager.has_store:
            # Revert to local store
            persistency_config = self.inventory_config.persistency
            self.inventory.init_store(persistency_config.module, persistency_config.config)
            # Save all that's loaded to local
            self.inventory.flush_to_store()

        # We are ready to serve
        self.manager.set_status(ServerManager.SRV_ONLINE)

    def serve_executables(self):
        """
        Infinite-loop main body of the daemon.
        Step 1: Poll the executables list for one uploaded script.
        Step 2: If a script is found, check the authorization of the script.
        Step 3: Spawn a child process for the script.
        Step 4: Apply updates sent by other servers.
        Step 5: Collect completed child processes. Get updates from the write-enabled child process if there is one.
        Step 6: Sleep for N seconds.
        """

        self.load_inventory()

        LOG.info('Started dynamo daemon.')

        child_processes = []

        # There can only be one child process with write access at a time. We pass it a Queue to communicate back.
        # writing_process is a tuple (proc, queue) when some process is writing
        writing_process = (0, None)

        try:
            LOG.info('Start polling for executables.')

            first_wait = True
            do_sleep = False

            while True:
                ## Check status and connection
                self.manager.check_status()
                if not self.manager.has_store and not self.inventory.check_store():
                    # We lost connection to the remote persistency store. Try another server.
                    # If there is no server to connect to, this method raises a RuntimeError
                    self.setup_remote_store()

                ## Step 4 (easier to do here because we use "continue"s)
                self.read_updates()

                ## Step 5 (easier to do here because we use "continue"s)
                writing_process = self.collect_processes(child_processes, writing_process)

                ## Step 6 (easier to do here because we use "continue"s)
                if do_sleep:
                    time.sleep(1)

                ## Step 1: Poll
                LOG.debug('Polling for executables.')

                next_executable = self.manager.get_next_executable()

                if next_executable is None:
                    if len(child_processes) == 0 and first_wait:
                        LOG.info('Waiting for executables.')
                        first_wait = False

                    do_sleep = True

                    LOG.debug('No executable found, sleeping for 1 second.')
                    continue

                ## Step 2: If a script is found, check the authorization of the script.
                exec_id, write_request, title, path, args, user_name = next_executable

                first_wait = True
                do_sleep = False

                if not os.path.exists(path + '/exec.py'):
                    LOG.info('Executable %s from user %s (write request: %s) not found.', title, user_name, write_request)
                    self.manager.set_executable_status(exec_id, ServerManager.EXC_NOTFOUND)
                    continue

                LOG.info('Found executable %s from user %s (write request: %s)', title, user_name, write_request)

                proc_args = (path, args)

                if write_request:
                    if not self.manager.check_write_auth(title, user_name, path):
                        LOG.warning('Executable %s from user %s is not authorized for write access.', title, user_name)
                        # TODO send a message

                        self.manager.set_executable_status(exec_id, ServerManager.EXC_AUTHFAILED)
                        continue

                    queue = multiprocessing.Queue()
                    proc_args += (queue,)

                    writing_process = (exec_id, queue)

                ## Step 3: Spawn a child process for the script
                self.manager.set_executable_status(exec_id, ServerManager.EXC_RUN)

                proc = multiprocessing.Process(target = self._run_one, name = title, args = proc_args)
                proc.daemon = True
                proc.start()

                LOG.info('Started executable %s (%s) from user %s (PID %d).', title, path, user_name, proc.pid)

                child_processes.append((exec_id, proc, user_name, path))

        except KeyboardInterrupt:
            LOG.info('Server process was interrupted.')

        except:
            # log the exception
            LOG.warning('Exception in server process. Terminating all child processes.')

            if self.manager.status != ServerManager.SRV_OUTOFSYNC:
                self.manager.set_status(ServerManager.SRV_ERROR)

            log_exception(LOG)
            raise

        finally:
            # If the main process was interrupted by Ctrl+C:
            # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
            # foreground process group). In this case calling terminate() will duplicate signals
            # in the child. Child processes have to always ignore SIGINT and be killed only from
            # SIGTERM sent by the line below.

            for exec_id, proc, user_name, path in child_processes:
                LOG.warning('Terminating %s (%s) requested by %s (PID %d)', proc.name, path, user_name, proc.pid)

                killproc(proc)

                if proc.is_alive():
                    LOG.warning('Child process %d did not return after 5 seconds.', proc.pid)

                self.manager.set_executable_status(exec_id, ServerManager.EXC_KILLED)

            if self.manager.status == ServerManager.SRV_OUTOFSYNC:
                # dynamod restarts this server
                self.inventory = DynamoInventory(self.inventory_config, load = False)
                self.manager.set_status(ServerManager.SRV_INITIAL)
            else:
                # Server is shutting down either in online or error state
                self.manager.disconnect()

    def setup_remote_store(self, hostname = ''):
        remote_store = self.manager.find_remote_store(hostname = hostname)
        if remote_store is None:
            self.manager.set_status(ServerManager.SRV_ERROR)
            raise RuntimeError('Could not find a remote persistency store to connect to.')

        module, config = remote_store
        self.inventory.init_store(module, config)

    def collect_processes(self, child_processes, writing_process):
        ichild = 0
        while ichild != len(child_processes):
            exec_id, proc, user_name, path = child_processes[ichild]

            status = self.manamger.get_executable_status(exec_id)
            if status == ServerManager.EXC_KILLED:
                killproc(proc)
                proc.join(60)

            if exec_id == writing_process[0]:
                if status == ServerManager.EXC_KILLED:
                    read_state = -1

                else: # i.e. status == RUN
                    # If this is the writing process, read data from the queue
                    # read_state: 0 -> nothing written yet (process is running), 1 -> read OK, 2 -> failure
                    read_state, update_commands = self.collect_updates(writing_process[1])
    
                    if read_state == 1:
                        status = ServerManager.EXC_DONE
    
                        # Block system signals and get update done
                        with SignalBlocker(logger = LOG):
                            self.exec_updates(update_commands)
    
                    elif read_state == 2:
                        status = ServerManager.EXC_FAILED
                        killproc(proc)

                if read_state != 0:
                    proc.join(60)
                    writing_process = (0, None)
    
            if proc.is_alive():
                if status == ServerManager.EXC_RUN:
                    ichild += 1
                    continue
                else:
                    # The process must be complete but did not join within 60 seconds
                    LOG.error('Executable %s (%s) from user %s is stuck (Status %s).', proc.name, path, user_name, ServerManager.executable_status_name(status))
            else:
                if status == ServerManager.EXC_RUN:
                    if proc.exitcode == 0:
                        status = ServerManager.EXC_DONE
                    else:
                        status = ServerManager.EXC_FAILED

                LOG.info('Executable %s (%s) from user %s completed (Exit code %d Status %s).', proc.name, path, user_name, proc.exitcode, ServerManager.executable_status_name(status))

            # process completed or is alive but stuck -> remove from the list and set status in the table
            child_processes.pop(ichild)

            self.manager.set_executable_status(exec_id, status, exit_code = proc.exitcode)

        return writing_process

    def collect_updates(self, queue):
        print_every = 1000
        updates_received = 0
        deletes_received = 0

        reading = False
        update_commands = []

        while True:
            try:
                # Once we have an item sent, we'll read until the end (EOM).
                # If the child dies in the middle of messaging, we get out of the while loop by timeout = 60
                cmd, objstr = queue.get(block = reading, timeout = 60)
            except Queue.Empty:
                if reading:
                    # The child process crashed or timed out
                    return 2, update_commands
                else:
                    return 0, update_commands
            else:
                reading = True # Now we have to read until the end - start blocking queue.get

                if LOG.getEffectiveLevel() == logging.DEBUG:
                    LOG.debug('From queue: %d %s', cmd, objstr)

                if cmd == DynamoInventory.CMD_UPDATE:
                    updates_received += 1
                    update_commands.append((cmd, objstr))
                elif cmd == DynamoInventory.CMD_DELETE:
                    deletes_received += 1
                    update_commands.append((cmd, objstr))

                if cmd == DynamoInventory.CMD_EOM or len(update_commands) % print_every == 0:
                    LOG.info('Received %d updates and %d deletes.', updates_received, deletes_received)

                if cmd == DynamoInventory.CMD_EOM:
                    return 1, update_commands

    def exec_updates(self, update_commands):
        # My updates
        self.manager.set_status(ServerManager.SRV_UPDATING)

        for cmd, objstr in update_commands:
            # Create a python object from its representation string
            obj = self.inventory.make_object(objstr)

            if cmd == DynamoInventory.CMD_UPDATE:
                self.inventory.update(obj, write = True, changelog = CHANGELOG)
            elif cmd == DynamoInventory.CMD_DELETE:
                CHANGELOG.info('Deleting %s', str(obj))
                self.inventory.delete(obj, write = True)

        self.manager.set_status(ServerManager.SRV_ONLINE)

        # Others
        self.manager.send_updates(update_commands)

    def read_updates(self):
        has_updates = False
        for cmd, objstr in self.manager.get_updates():
            has_updates = True

            # Create a python object from its representation string
            obj = self.inventory.make_object(objstr)

            if cmd == 'update':
                self.inventory.update(obj, write = True, changelog = CHANGELOG)
            elif cmd == DynamoInventory.CMD_DELETE:
                CHANGELOG.info('Deleting %s', str(obj))
                self.inventory.delete(obj, write = True)

        if has_updates:
            # The server which sent the updates has set this server's status to updating
            self.manager.set_status(ServerManager.SRV_ONLINE)
      
    def _run_one(self, path, args, queue = None):
        # Set the uid of the process
        os.seteuid(0)
        os.setegid(0)

        if queue is None:
            pwnam = pwd.getpwnam(self.read_user)
        else:
            pwnam = pwd.getpwnam(self.full_user)

        os.setgid(pwnam.pw_gid)
        os.setuid(pwnam.pw_uid)

        # Redirect STDOUT and STDERR to file, close STDIN
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = open(path + '/_stdout', 'a')
        sys.stderr = open(path + '/_stderr', 'a')
        sys.stdin.close()

        ## Ignore SIGINT - see note above proc.terminate()
        ## We will react to SIGTERM by raising KeyboardInterrupt
        from dynamo.utils.signaling import SignalConverter
        
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        signal_converter = SignalConverter()
        signal_converter.set(signal.SIGTERM)

        # Set argv
        sys.argv = [path + '/exec.py']
        if args:
            sys.argv += shlex.split(args) # split using shell-like syntax

        # Reset logging
        # This is a rather hacky solution relying perhaps on the implementation internals of
        # the logging module. It might stop working with changes to the logging.
        # The assumptions are:
        #  1. All loggers can be reached through Logger.manager.loggerDict
        #  2. All logging.shutdown() does is call flush() and close() over all handlers
        #     (i.e. calling the two is enough to ensure clean cutoff from all resources)
        #  3. root_logger.handlers is the only link the root logger has to its handlers
        for logger in [logging.getLogger()] + logging.Logger.manager.loggerDict.values():
            while True:
                try:
                    handler = logger.handlers.pop()
                except AttributeError:
                    # logger is just a PlaceHolder and does not have .handlers
                    break
                except IndexError:
                    break
    
                handler.flush()
                handler.close()

        # Re-initialize inventory store with read-only connection
        # This is for security and simply for concurrency - multiple processes
        # should not share the same DB connection
        if self.manager.has_store:
            self.inventory.disable_store_write()
        else:
            self.setup_remote_store(self.manager.store_host)

        # Pass my registry and inventory to the executable through core.executable
        import dynamo.core.executable as executable
        executable.inventory = self.inventory

        if queue is not None:
            executable.read_only = False
            # create a list of updated and deleted objects the executable can fill
            executable.inventory._update_commands = []

        try:
            execfile(path + '/exec.py', {'__name__': '__main__'})
        except SystemExit as exc:
            if exc.code == 0:
                pass
            else:
                raise

        if queue is not None:
            nobj = len(self.inventory._update_commands)
            sys.stderr.write('Sending %d updated objects to the server process.\n' % nobj)
            sys.stderr.flush()
            wm = 0.
            for iobj, (cmd, objstr) in enumerate(self.inventory._update_commands):
                if float(iobj) / nobj * 100. > wm:
                    sys.stderr.write(' %.0f%%..' % (float(iobj) / nobj * 100.))
                    sys.stderr.flush()
                    wm += 5.

                try:
                    queue.put((cmd, objstr))
                except:
                    sys.stderr.write('Exception while sending %s %s\n' % (DynamoInventory._cmd_str[cmd], objstr))
                    raise

            if nobj != 0:
                sys.stderr.write(' 100%.\n')
                sys.stderr.flush()
            
            # Put end-of-message
            queue.put((DynamoInventory.CMD_EOM, None))

        # Queue stays available on the other end even if we terminate the process

        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = stdout
        sys.stderr = stderr

        return 0
