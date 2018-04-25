import os
import sys
import shutil
import time
import logging
import shlex
import signal
import code
import multiprocessing
import threading
import Queue

from dynamo.core.inventory import DynamoInventory
from dynamo.core.manager import ServerManager, OutOfSyncError
import dynamo.core.serverutils as serverutils
from dynamo.core.components.appserver import AppServer
from dynamo.utils.signaling import SignalBlocker
from dynamo.dataformat.exceptions import log_exception

LOG = logging.getLogger(__name__)
CHANGELOG = logging.getLogger('changelog')

BANNER = '''
+++++++++++++++++++++++++++++++++++++
++++++++++++++ DYNAMO +++++++++++++++
++++++++++++++  v2.1  +++++++++++++++
+++++++++++++++++++++++++++++++++++++
'''

class DynamoServer(object):
    """Main daemon class."""

    def __init__(self, config):
        LOG.info('Initializing Dynamo server %s.', __file__)

        ## User name
        self.user = config.user

        ## Create the inventory
        self.inventory_config = config.inventory.clone()
        self.inventory = None

        ## Create the server manager
        self.manager_config = config.manager.clone()
        self.manager = ServerManager(self.manager_config)

        ## Application collection
        self.applications_config = config.applications.clone()

        ## Server status (and application) poll interval
        self.poll_interval = config.status_poll_interval

        ## Load the inventory content (filter according to debug config)
        self.inventory_load_opts = {}
        if 'debug' in config:
            for objs in ['groups', 'sites', 'datasets']:
                included = config.debug.get('included_' + objs, None)
                excluded = config.debug.get('excluded_' + objs, None)
    
                self.inventory_load_opts[objs] = (included, excluded)

        ## Maximum number of consecutive unhandled errors before shutting down the server
        self.max_num_errors = config.max_num_errors
        self.num_errors = 0

        ## How long application workspaces are retained (days)
        self.applications_keep = config.applications_keep * 3600 * 24

        ## Shutdown flag
        # Default is set. KeyboardInterrupt is raised when flag is cleared
        self.shutdown_flag = threading.Event()
        self.shutdown_flag.set()

    def load_inventory(self):
        self.inventory = DynamoInventory(self.inventory_config)

        ## Wait until there is no write process
        while True:
            if self.manager.master.get_writing_process_id() is not None:
                LOG.debug('A write-enabled process is running. Checking again in 5 seconds.')
                time.sleep(5)
            else:
                break

        ## Write process is done.
        ## Other servers will not start a new write process while there is a server with status 'starting'.
        ## The only states the other running servers can be in are therefore 'updating' or 'online'

        if self.manager.count_servers(ServerManager.SRV_ONLINE) == 0:
            # I am the first server to start the inventory - need to have a store.
            if not self.inventory.has_store():
                raise RuntimeError('No persistent inventory storage is available.')
        else:
            if self.inventory.has_store():
                # Clone the content from a remote store
                hostname, module, config = self.manager.find_remote_store()
                LOG.info('Cloning inventory content from persistency store at %s', hostname)
                self.inventory.clone_store(module, config)
            else:
                self.setup_remote_store()

        LOG.info('Loading the inventory.')
        self.inventory.load(**self.inventory_load_opts)

        LOG.info('Inventory is ready.')

    def run(self):
        """
        Main body of the server, but mostly focuses on exception handling. dynamod runs this function
        in a non-main thread.
        """

        # Number of unhandled errors
        self.num_errors = 0

        # Outer loop: restart the application server when error occurs
        while True:
            try:
                # Lock write activities by other servers
                self.manager.set_status(ServerManager.SRV_STARTING)

                self.load_inventory()

                bconf = self.manager_config.board
                self.manager.master.advertise_board(bconf.module, bconf.config)

                if self.inventory.has_store():
                    pconf = self.inventory_config.persistency
                    self.manager.master.advertise_store(pconf.module, pconf.readonly_config)

                if self.manager.shadow is not None:
                    sconf = self.manager_config.shadow
                    self.manager.master.advertise_shadow(sconf.module, sconf.config)

                # We are ready to work
                self.manager.set_status(ServerManager.SRV_ONLINE)

                # Actual stuff happens here
                if self.applications_config.enabled:
                    self._run_application_cycles()
                else:
                    self._run_update_cycles()

            except KeyboardInterrupt:
                LOG.info('Server process was interrupted.')
                # KeyboardInterrupt is raised when shutdown_flag is set
                # Notify shutdown ready
                self.shutdown_flag.set()
                return
    
            except OutOfSyncError:
                LOG.error('Server has gone out of sync with its peers.')
                log_exception(LOG)
   
            except:
                log_exception(LOG)
                self.num_errors += 1

            if self.num_errors >= self.max_num_errors:
                LOG.error('Consecutive %d errors occurred. Shutting down Dynamo.' % self.num_errors)
                os.kill(os.getpid(), signal.SIGINT)
                self.shutdown_flag.clear()
                return

            if not self.manager.master.connected:
                # We need to reconnect to another server
                LOG.error('Lost connection to the master server.')
                self.manager.reconnect_master()
    
            # set server status to initial
            try:
                self.manager.reset_status()
            except:
                self.manager.status = ServerManager.SRV_INITIAL

    def _run_application_cycles(self):
        """
        Infinite-loop main body of the daemon.
        Step 1: Poll the applications list for one uploaded script.
        Step 2: If a script is found, check the authorization of the script.
        Step 3: Spawn a child process for the script.
        Step 4: Apply updates sent by other servers.
        Step 5: Collect completed child processes. Get updates from the write-enabled child process if there is one.
        Step 6: Clean up.
        Step 7: Sleep for N seconds.
        """

        # Start the application collector thread
        aconf = self.applications_config.server
        appserver = AppServer.get_instance(aconf.module, self, aconf.config)
        appserver.start()

        LOG.info('Start polling for applications.')

        child_processes = []

        try:
            # There can only be one child process with write access at a time. We pass it a Queue to communicate back.
            # writing_process is a tuple (proc, queue) when some process is writing
            writing_process = (0, None)
    
            first_wait = True
            do_sleep = False

            while True:
                LOG.debug('Check status and connection')
                self.check_status_and_connection()
    
                ## Step 4 (easier to do here because we use "continue"s)
                LOG.debug('Read updates')
                self.read_updates()
    
                ## Step 5 (easier to do here because we use "continue"s)
                LOG.debug('Collect processes')
                writing_process = self.collect_processes(child_processes, writing_process, appserver)

                ## Step 6 (easier to do here because we use "continue"s)
                LOG.debug('Clean old workareas')
                self.cleanup()
    
                ## Step 7 (easier to do here because we use "continue"s)
                if do_sleep:
                    # one successful cycle - reset the error counter
                    self.num_errors = 0

                    LOG.debug('Sleep %d', self.poll_interval)
                    time.sleep(self.poll_interval)
    
                ## Step 1: Poll
                LOG.debug('Polling for applications.')
    
                next_application = self.manager.get_next_application()
    
                if next_application is None:
                    if len(child_processes) == 0 and first_wait:
                        LOG.info('Waiting for applications.')
                        first_wait = False
    
                    do_sleep = True
    
                    LOG.debug('No application found, sleeping for %.1f second(s).' % self.poll_interval)
                    continue
    
                ## Step 2: If a script is found, check the authorization of the script.
                app_id, write_request, title, path, args, user_name = next_application
    
                first_wait = True
                do_sleep = False

                if not os.path.exists(path + '/exec.py'):
                    LOG.info('Application %s from user %s (write request: %s) not found.', title, user_name, write_request)
                    self.manager.master.update_application(app_id, status = ServerManager.APP_NOTFOUND)
                    appserver.notify_synch_app(app_id, {'status': ServerManager.APP_NOTFOUND})
                    continue
    
                LOG.info('Found application %s from user %s (write request: %s)', title, user_name, write_request)
    
                proc_args = (path, args)
    
                if write_request:
                    if not self.manager.check_write_auth(title, user_name, path):
                        LOG.warning('Application %s from user %s is not authorized for write access.', title, user_name)
                        # TODO send a message
    
                        self.manager.master.update_application(app_id, status = ServerManager.APP_AUTHFAILED)
                        appserver.notify_synch_app(app_id, {'status': ServerManager.APP_AUTHFAILED})
                        continue
    
                    queue = multiprocessing.Queue()
                    proc_args += (queue,)
    
                    writing_process = (app_id, queue)
    
                ## Step 3: Spawn a child process for the script
                self.manager.master.update_application(app_id, status = ServerManager.APP_RUN)
    
                proc = multiprocessing.Process(target = self.run_script, name = title, args = proc_args)
                proc.daemon = True
                proc.start()

                appserver.notify_synch_app(app_id, {'status': ServerManager.APP_RUN, 'path': path, 'pid': proc.pid})
    
                LOG.info('Started application %s (%s) from user %s (PID %d).', title, path, user_name, proc.pid)
    
                child_processes.append((app_id, proc, user_name, path))

        except KeyboardInterrupt:
            if len(child_processes) != 0:
                LOG.info('Terminating all child processes..')
            raise

        except:
            if len(child_processes) != 0:
                LOG.error('Exception in server process. Terminating all child processes..')
            else:
                LOG.error('Exception in server process.')

            if self.manager.status not in [ServerManager.SRV_OUTOFSYNC, ServerManager.SRV_ERROR]:
                try:
                    self.manager.set_status(ServerManager.SRV_ERROR)
                except:
                    pass

            raise

        finally:
            # If the main process was interrupted by Ctrl+C:
            # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
            # foreground process group). In this case calling terminate() will duplicate signals
            # in the child. Child processes have to always ignore SIGINT and be killed only from
            # SIGTERM sent by the line below.

            for app_id, proc, user_name, path in child_processes:
                LOG.warning('Terminating %s (%s) requested by %s (PID %d)', proc.name, path, user_name, proc.pid)

                serverutils.killproc(proc)

                try:
                    self.manager.master.update_application(app_id, status = ServerManager.APP_KILLED)
                except:
                    pass

            LOG.info('Stopping application server.')
            # Close the application collector. The collector thread will terminate
            appserver.stop()

    def _run_update_cycles(self):
        """
        Infinite-loop main body of the daemon.
        Step 1: Apply updates sent by other servers.
        Step 2: Sleep for N seconds.
        """

        LOG.info('Start checking for updates.')

        try:
            while True:
                self.check_status_and_connection()
    
                ## Step 1
                self.read_updates()

                # one successful cycle - reset the error counter
                self.num_errors = 0
    
                ## Step 2
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            raise

        except:
            LOG.error('Exception in server process.')

            if self.manager.status not in [ServerManager.SRV_OUTOFSYNC, ServerManager.SRV_ERROR]:
                try:
                    self.manager.set_status(ServerManager.SRV_ERROR)
                except:
                    pass

            raise

    def check_status_and_connection(self):
        if not self.shutdown_flag.is_set():
            raise KeyboardInterrupt('Shutdown')

        ## Check status (raises exception if error)
        self.manager.check_status()
    
        if not self.inventory.check_store():
            # We lost connection to the remote persistency store. Try another server.
            # If there is no server to connect to, this method raises a RuntimeError
            self.setup_remote_store()

    def setup_remote_store(self, hostname = ''):
        # find_remote_store raises a RuntimeError if not source is found
        hostname, module, config = self.manager.find_remote_store(hostname = hostname)
        LOG.info('Using persistency store at %s', hostname)
        self.inventory.init_store(module, config)

    def collect_processes(self, child_processes, writing_process, appserver):
        ichild = 0
        while ichild != len(child_processes):
            app_id, proc, user_name, path = child_processes[ichild]

            apps = self.manager.master.get_applications(app_id = app_id)
            if len(apps) == 0:
                status = ServerManager.APP_KILLED
            else:
                status = apps[0]['status']
            
            if status == ServerManager.APP_KILLED:
                serverutils.killproc(proc)

            read_only = True

            if app_id == writing_process[0]:
                read_only = False

                if status == ServerManager.APP_KILLED:
                    read_state = -1

                else: # i.e. status == RUN
                    # If this is the writing process, read data from the queue
                    # read_state: 0 -> nothing written yet (process is running), 1 -> read OK, 2 -> failure
                    read_state, update_commands = self.collect_updates(writing_process[1])
    
                    if read_state == 1:
                        status = ServerManager.APP_DONE
    
                        # Block system signals and get update done
                        with SignalBlocker(logger = LOG):
                            self.exec_updates(update_commands)
    
                    elif read_state == 2:
                        status = ServerManager.APP_FAILED
                        serverutils.killproc(proc)

                if read_state != 0:
                    proc.join(60)
                    writing_process = (0, None)
    
            if proc.is_alive():
                if status == ServerManager.APP_RUN:
                    ichild += 1
                    continue
                else:
                    # The process must be complete but did not join within 60 seconds
                    LOG.error('Application %s (%s) from user %s is stuck (Status %s).', proc.name, path, user_name, ServerManager.application_status_name(status))
            else:
                if status == ServerManager.APP_RUN:
                    if proc.exitcode == 0:
                        status = ServerManager.APP_DONE
                    else:
                        status = ServerManager.APP_FAILED

                LOG.info('Application %s (%s) from user %s completed (Exit code %d Status %s).', proc.name, path, user_name, proc.exitcode, ServerManager.application_status_name(status))

               
            child_processes.pop(ichild)

            appserver.notify_synch_app(app_id, {'status': status, 'exit_code': proc.exitcode})

            self.manager.master.update_application(app_id, status = status, exit_code = proc.exitcode)

            if read_only:
                self.clean_readonly(path)

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

    def cleanup(self):
        applications = self.manager.master.get_applications(older_than = self.applications_keep)

        read_only_paths = []
        for app_id, write_request, title, path, args, user_name in applications:
            if not os.path.isdir(path):
                continue

            if not write_request:
                read_only_paths.append(path)

        # First make sure all mounts are removed.
        for path in read_only_paths:
            self.clean_readonly(path)

        for app_id, write_request, title, path, args, user_name in applications:
            shutil.rmtree(path)
            self.manager.master.update_application(app_id = app_id, path = None)

    def clean_readonly(self, path):
        # Since threads cannot change the uid, we launch a subprocess.
        # (Mounts are made read-only, so there is no risk of accidents even if the subprocess fails)
        proc = multiprocessing.Process(target = serverutils.umountall, args = (path,))
        proc.start()
        proc.join()

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

    def run_script(self, path, args, queue = None):
        """
        Main function for script execution.
        @param path   Path to the work area of the script. Will be the root directory in read-only processes.
        @param args   Script command-line arguments.
        @param queue  Queue if write-enabled.
        """

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        stdout = open(path + '/_stdout', 'a')
        stderr = open(path + '/_stderr', 'a')
        sys.stdout = stdout
        sys.stderr = stderr

        path = self._pre_execution(path, queue is None)

        # Set argv
        sys.argv = [path + '/exec.py']
        if args:
            sys.argv += shlex.split(args) # split using shell-like syntax

        # Execute the script
        try:
            myglobals = {'__builtins__': __builtins__, '__name__': '__main__', '__file__': 'exec.py', '__doc__': None, '__package__': None}
            execfile(path + '/exec.py', myglobals)
        except SystemExit as exc:
            if exc.code == 0:
                pass
            else:
                raise
        finally:
            self._post_execution(queue)

        sys.stdout = old_stdout
        sys.stderr = old_stderr
        stdout.close()
        stderr.close()

        # Queue stays available on the other end even if we terminate the process

        return 0

    def run_interactive(self, path, stdout = sys.stdout, stderr = sys.stderr, make_console = code.InteractiveConsole):
        """
        Main function for interactive sessions.
        For now we limit interactive sessions to read-only.
        @param path         Path to the work area.
        @param stdout       File-like object for stdout
        @param stderr       File-like object for stderr
        @param make_console Callable which takes a dictionary of locals as an argument and returns a console
        """

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr

        self._pre_execution(path, True)

        # use receive of oconn as input
        mylocals = {'__builtins__': __builtins__, '__name__': '__main__', '__doc__': None, '__package__': None, 'inventory': self.inventory}
        console = make_console(mylocals)
        try:
            console.interact(BANNER)
        finally:
            self._post_execution(None)

        sys.stdout = old_stdout
        sys.stderr = old_stderr

        return 0

    def _pre_execution(self, path, read_only):
        uid = os.geteuid()
        gid = os.getegid()

        if read_only:
            # Set defaults
            for key, config in self.applications_config.defaults.items():
                try:
                    myconf = config['readonly']
                except KeyError:
                    myconf = config['all']
                else:
                    # security measure
                    del config['fullauth']

                modname, clsname = key.split(':')
                module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
                cls = getattr(module, clsname)

                cls.set_default(myconf)

            # Confine in a chroot jail
            # Allow access to directories in PYTHONPATH with bind mounts
            for base in serverutils.mountpoints:
                try:
                    os.makedirs(path + base)
                except OSError:
                    # shouldn't happen but who knows
                    continue

                serverutils.bindmount(base, path + base)

            os.mkdir(path + '/tmp')
            os.chmod(path + '/tmp', 0777)

            os.seteuid(0)
            os.setegid(0)
            os.chroot(path)

            path = ''
            os.chdir('/')

        else:
            # Set defaults
            for key, config in self.applications_config.defaults.items():
                try:
                    myconf = config['fullauth']
                except KeyError:
                    myconf = config['all']

                modname, clsname = key.split(':')
                module = __import__(modname, globals(), locals())
                cls = getattr(module, clsname)

                cls.set_default(myconf)

            os.chdir(path)

        # De-escalate privileges permanently
        os.seteuid(0)
        os.setegid(0)
        os.setgid(uid)
        os.setuid(gid)

        # Ignore SIGINT - see note above proc.terminate()
        # We will react to SIGTERM by raising KeyboardInterrupt
        from dynamo.utils.signaling import SignalConverter
        
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        signal_converter = SignalConverter()
        signal_converter.set(signal.SIGTERM)
        # we won't call unset()

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
        if self.inventory.has_store():
            pconf = self.inventory_config.persistency
            self.inventory.init_store(pconf.module, pconf.readonly_config)
        else:
            self.setup_remote_store(self.manager.store_host)

        # Pass my registry and inventory to the executable through core.executable
        import dynamo.core.executable as executable
        executable.inventory = self.inventory

        if not read_only:
            executable.read_only = False
            # create a list of updated and deleted objects the executable can fill
            executable.inventory._update_commands = []

        return path

    def _post_execution(self, queue):
        if queue is not None:
            # Collect updates if write-enabled

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

    def shutdown(self):
        LOG.info('Shutting down Dynamo server..')

        if self.shutdown_flag.is_set():
            self.shutdown_flag.clear()
            state = self.shutdown_flag.wait(60)
            if not state:
                # timed out
                LOG.warning('Shutdown timeout of 60 seconds have passed.')

        self.manager.disconnect()
