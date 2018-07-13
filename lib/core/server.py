import os
import sys
import shutil
import time
import logging
import signal
import code
import hashlib
import multiprocessing
import threading
import Queue
import traceback
import shlex

from dynamo.core.inventory import DynamoInventory
from dynamo.core.manager import ServerManager
import dynamo.core.serverutils as serverutils
from dynamo.core.components.appserver import AppServer
from dynamo.core.components.host import ServerHost, OutOfSyncError
from dynamo.core.components.appmanager import AppManager
from dynamo.web.server import WebServer
from dynamo.fileop.rlfsm import RLFSM
from dynamo.utils.log import log_exception, reset_logger
from dynamo.utils.signaling import SignalBlocker
from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)
CHANGELOG = logging.getLogger('changelog')

class DynamoServer(object):
    """Main daemon class."""

    def __init__(self, config):
        LOG.info('Initializing Dynamo server %s.', __file__)

        ## Create the inventory
        self.inventory_config = config.inventory.clone()
        self.inventory = None

        ## Create the server manager
        self.manager_config = config.manager.clone()
        self.manager = ServerManager(self.manager_config)

        ## Modules defaults config
        self.defaults_config = config.defaults

        ## Application collection
        self.applications_config = config.applications.clone()
        if self.applications_config.enabled:
            # Initialize the appserver since it may require elevated privilege (this Ctor is run as root)
            aconf = self.applications_config.server
            self.appserver = AppServer.get_instance(aconf.module, self, aconf.config)

            if self.applications_config.timeout < 60:
                # Some errors were observed when the timeout is too short
                # (probably 1 second is enough - we just need to get through pre_execution)
                self.applications_config.timeout = 60

        ## Web server
        if config.web.enabled:
            config.web.modules_config = Configuration(config.web.modules_config_path)
            config.web.pop('modules_config_path')
    
            self.webserver = WebServer(config.web, self)
        else:
            self.webserver = None

        ## File Operations Manager
        if config.file_operations.enabled:
            self.fom = RLFSM(config.file_operations.manager)
        else:
            self.fom = None

        ## Server status (and application) poll interval
        self.poll_interval = config.status_poll_interval

        ## Load the inventory content (filter according to debug config)
        self.inventory_load_opts = {}
        if 'debug' in config:
            for objs in ['groups', 'sites', 'datasets']:
                included = config.debug.get('included_' + objs, None)
                excluded = config.debug.get('excluded_' + objs, None)
    
                self.inventory_load_opts[objs] = (included, excluded)

        ## Queue to send / receive inventory updates
        self.inventory_update_queue = multiprocessing.JoinableQueue()

        ## Recipient of error message emails
        self.notification_recipient = config.notification_recipient

    def load_inventory(self):
        self.inventory = DynamoInventory(self.inventory_config)

        ## Wait until there is no write process
        while self.manager.master.get_writing_process_id() is not None:
            LOG.debug('A write-enabled process is running. Checking again in 5 seconds.')
            time.sleep(5)

        ## Write process is done.
        ## Other servers will not start a new write process while there is a server with status 'starting'.
        ## The only states the other running servers can be in are therefore 'updating' or 'online'
        while self.manager.count_servers(ServerHost.STAT_UPDATING) != 0:
            time.sleep(2)

        if self.manager.count_servers(ServerHost.STAT_ONLINE) == 0:
            # I am the first server to start the inventory - need to have a store.
            if not self.inventory.has_store:
                raise RuntimeError('No persistent inventory storage is available.')
        else:
            # find_remote_store raises a RuntimeError if no source is found
            hostname, module, config, version = self.manager.find_remote_store()

            if self.inventory.has_store:
                # Clone the content from a remote store

                # No server will be updating because write process is blocked while we load
                if version == self.inventory.store_version():
                    LOG.info('Local persistency store is up to date.')
                else:
                    # TODO cloning can take hours; need a way to unblock other servers and pool the updates
                    LOG.info('Cloning inventory content from persistency store at %s', hostname)
                    self.inventory.clone_store(module, config)
            else:
                # Use this remote store as mine (read-only)
                self._setup_remote_store(hostname, module, config)

        LOG.info('Loading the inventory.')
        self.inventory.load(**self.inventory_load_opts)

        LOG.info('Inventory is ready.')

    def run(self):
        """
        Main body of the server, but mostly focuses on exception handling.
        """

        # Outer loop: restart the application server when the inventory goes out of synch
        while True:
            # Lock write activities by other servers
            self.manager.set_status(ServerHost.STAT_STARTING)

            self.load_inventory()

            bconf = self.manager_config.board
            self.manager.master.advertise_board(bconf.module, bconf.config)

            if self.inventory.has_store:
                pconf = self.inventory_config.persistency
                self.manager.master.advertise_store(pconf.module, pconf.readonly_config)
                self.manager.master.advertise_store_version(self.inventory.store_version())

            if self.manager.shadow is not None:
                sconf = self.manager_config.shadow
                self.manager.master.advertise_shadow(sconf.module, sconf.config)

            # We are ready to work
            self.manager.set_status(ServerHost.STAT_ONLINE)

            if self.webserver:
                self.webserver.start()

            if self.fom:
                self.fom.start(self.inventory)

            try:
                # Actual stuff happens here
                # Both functions are infinite loops; the only way out is an exception (can be a peaceful KeyboardInterrupt)
                if self.applications_config.enabled:
                    self._run_application_cycles()
                else:
                    self._run_update_cycles()

            except KeyboardInterrupt:
                LOG.info('Server process was interrupted.')

                break
    
            except OutOfSyncError:
                LOG.error('Server has gone out of sync with its peers.')
                log_exception(LOG)

                if not self.manager.master.connected:
                    # We need to reconnect to another server
                    LOG.error('Lost connection to the master server.')
                    self.manager.reconnect_master()
        
                # set server status to initial
                try:
                    self.manager.reset_status()
                except:
                    self.manager.status = ServerHost.STAT_INITIAL
   
            except:
                log_exception(LOG)
                LOG.error('Shutting down Dynamo.')

                break

            finally:
                if self.webserver:
                    self.webserver.stop()

                if self.fom:
                    self.fom.stop()

        self.manager.disconnect()

    def check_status_and_connection(self):
        ## Check status (raises exception if error)
        self.manager.check_status()
    
        if self.inventory is not None and not self.inventory.check_store():
            # We lost connection to the remote persistency store. Try another server.
            # If there is no server to connect to, this method raises a RuntimeError
            hostname, module, config, version = self.manager.find_remote_store()
            self._setup_remote_store(hostname, module, config)

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
        self.appserver.start()

        child_processes = []

        LOG.info('Start polling for applications.')

        try:
            first_wait = True
            do_sleep = False
            cleanup_timer = 0

            while True:
                LOG.debug('Check status and connection')
                self.check_status_and_connection()
    
                ## Step 4 (easier to do here because we use "continue"s)
                LOG.debug('Read updates')
                self._read_updates()
    
                ## Step 5 (easier to do here because we use "continue"s)
                LOG.debug('Collect processes')
                self._collect_processes(child_processes)

                if self.webserver is not None:
                    self._collect_updates_from_web()

                ## Step 6 (easier to do here because we use "continue"s)
                cleanup_timer += 1
                if cleanup_timer == 100000:
                    LOG.info('Triggering cleanup of old applications.')
                    self._cleanup()
                    cleanup_timer = 0
    
                ## Step 7 (easier to do here because we use "continue"s)
                if do_sleep:
                    # one successful cycle - reset the error counter
                    LOG.debug('Sleep ' + str(self.poll_interval))
                    time.sleep(self.poll_interval)
    
                ## Step 1: Poll
                LOG.debug('Polling for applications.')

                self.manager.master.lock()
                try:
                    # Cannot run a write process if
                    #  . I am supposed to be updating my inventory
                    #  . There is a server starting
                    #  . There is already a write process
                    read_only = self.manager.master.inhibit_write()

                    app = self.manager.master.get_next_application(read_only)
                    if app is not None:
                        self.manager.master.update_application(app['appid'], status = AppManager.STAT_ASSIGNED, hostname = self.manager.hostname)
        
                finally:
                    self.manager.master.unlock()
    
                if app is None:
                    if len(child_processes) == 0 and first_wait:
                        LOG.info('Waiting for applications.')
                        first_wait = False
    
                    do_sleep = True
    
                    LOG.debug('No application found, sleeping for %.1f second(s).' % self.poll_interval)
                    continue
    
                ## Step 2: If a script is found, check the authorization of the script.
                first_wait = True
                do_sleep = False

                if not os.path.exists(app['path'] + '/exec.py'):
                    LOG.info('Application %s from %s@%s (auth level: %s) not found.', app['title'], app['user_name'], app['user_host'], AppManager.auth_level_name(app['auth_level']))
                    self.manager.master.update_application(app['appid'], status = AppManager.STAT_NOTFOUND)
                    self.appserver.notify_synch_app(app['appid'], {'status': AppManager.STAT_NOTFOUND})
                    continue
    
                LOG.info('Found application %s from %s (AID %s, auth level: %s)', app['title'], app['user_name'], app['appid'], AppManager.auth_level_name(app['auth_level']))

                is_local = (app['user_host'] == self.manager.hostname)
    
                if app['auth_level'] == AppManager.LV_WRITE:
                    # check authorization
                    with open(app['path'] + '/exec.py') as source:
                        checksum = hashlib.md5(source.read()).hexdigest()

                    if not self.manager.master.check_application_auth(app['title'], app['user_name'], checksum):
                        LOG.warning('Application %s from %s is not authorized for write access.', app['title'], app['user_name'])
                        # TODO send a message
    
                        self.manager.master.update_application(app['appid'], status = AppManager.STAT_AUTHFAILED)
                        self.appserver.notify_synch_app(app['appid'], {'status': AppManager.STAT_AUTHFAILED})
                        continue
    
                    writing_process = app['appid']

                ## Step 3: Spawn a child process for the script
                self.manager.master.update_application(app['appid'], status = AppManager.STAT_RUN)

                proc = self._start_subprocess(app, is_local)
                
                self.appserver.notify_synch_app(app['appid'], {'status': AppManager.STAT_RUN, 'path': app['path'], 'pid': proc.pid})
    
                LOG.info('Started application %s (%s) from %s@%s (AID %d PID %d).', app['title'], app['path'], app['user_name'], app['user_host'], app['appid'], proc.pid)
    
                child_processes.append((app['appid'], proc, app['user_name'], app['user_host'], app['path'], time.time()))

        except KeyboardInterrupt:
            if len(child_processes) != 0:
                LOG.info('Terminating all child processes..')

            raise

        except:
            if len(child_processes) != 0:
                LOG.error('Exception (%s) in server process. Terminating all child processes..', sys.exc_info()[0].__name__)
            else:
                LOG.error('Exception (%s) in server process.', sys.exc_info()[0].__name__)

            if self.manager.status not in [ServerHost.STAT_OUTOFSYNC, ServerHost.STAT_ERROR]:
                try:
                    self.manager.set_status(ServerHost.STAT_ERROR)
                except:
                    pass

            raise

        finally:
            # If the main process was interrupted by Ctrl+C:
            # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
            # foreground process group). In this case calling terminate() will duplicate signals
            # in the child. Child processes have to always ignore SIGINT and be killed only from
            # SIGTERM sent by the line below.

            for app_id, proc, user_name, user_host, path, time_start in child_processes:
                LOG.warning('Terminating %s (%s) from %s@%s (AID %d PID %d)', proc.name, path, user_name, user_host, app_id, proc.pid)

                serverutils.killproc(proc, LOG)

                try:
                    self.manager.master.update_application(app_id, status = AppManager.STAT_KILLED)
                except:
                    pass

            LOG.info('Stopping application server.')
            # Close the application collector. The collector thread will terminate
            self.appserver.stop()

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
                self._read_updates()

                if self.webserver is not None:
                    self._collect_updates_from_web()
    
                ## Step 2
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            raise

        except:
            LOG.error('Exception in server process.')

            if self.manager.status not in [ServerHost.STAT_OUTOFSYNC, ServerHost.STAT_ERROR]:
                try:
                    self.manager.set_status(ServerHost.STAT_ERROR)
                except:
                    pass

            raise

    def _setup_remote_store(self, hostname, module, config):
        LOG.info('Using persistency store at %s', hostname)
        self.manager.register_remote_store(hostname)
        self.inventory.init_store(module, config)

    def _collect_processes(self, child_processes):
        """
        Loop through child processes and make state machine transitions.
        Processes come in this function in status RUN or KILLED. It is also possible that
        the master server somehow lost the record of the process (which we considered KILLED).
        If the process times out, status is set to KILLED.
        KILLED jobs will be terminated and popped out of the child_processes list.
        RUN jobs will be polled. If not alive, status changes to DONE or FAILED depending on
        the exit code. If alive, nothing happens.
        In either case, for write-enabled processes, updates are collected from the queue.
        If the status is RUN and collection fails, the subprocess is terminated and the status
        is set to FAILED.
        """

        writing_process = self.manager.master.get_writing_process_id()

        ichild = 0
        while ichild != len(child_processes):
            app_id, proc, user_name, user_host, path, time_start = child_processes[ichild]

            id_str = '%s (%s) from %s@%s (AID %d PID %d)' % (proc.name, path, user_name, user_host, app_id, proc.pid)

            apps = self.manager.master.get_applications(app_id = app_id)
            if len(apps) == 0:
                status = AppManager.STAT_KILLED
            else:
                status = apps[0]['status']

            # Kill processes running for too long (timeout given in seconds)
            if time_start < time.time() - self.applications_config.timeout:
                LOG.warning('Application %s timed out.', id_str)
                status = AppManager.STAT_KILLED

            if app_id == writing_process:
                # If this is the writing process, read data from the queue
                # read_state: 0 -> nothing written yet (process is running), 1 -> read OK, 2 -> failure
                read_state, update_commands = self._collect_updates()

                if status == AppManager.STAT_RUN:
                    if read_state == 1 and len(update_commands) != 0:
                        self._update_inventory(update_commands)
    
                    elif read_state == 2:
                        status = AppManager.STAT_FAILED
                        serverutils.killproc(proc, LOG, 60)

            if status == AppManager.STAT_KILLED and proc.is_alive():
                LOG.warning('Terminating %s.', id_str)
                serverutils.killproc(proc, LOG, 60)

            if proc.is_alive():
                if status == AppManager.STAT_RUN:
                    ichild += 1
                    continue
                else:
                    # The process must be complete but did not join within 60 seconds
                    LOG.error('Application %s is stuck (Status %s).', id_str, AppManager.status_name(status))
            else:
                if status == AppManager.STAT_RUN:
                    if proc.exitcode == 0:
                        status = AppManager.STAT_DONE
                    else:
                        status = AppManager.STAT_FAILED

                LOG.info('Application %s completed (Exit code %d Status %s).', id_str, proc.exitcode, AppManager.status_name(status))
               
            child_processes.pop(ichild)

            self.appserver.notify_synch_app(app_id, {'status': status, 'exit_code': proc.exitcode})

            self.manager.master.update_application(app_id, status = status, exit_code = proc.exitcode)

    def _collect_updates(self):
        print_every = 100000
        updates_received = 0
        deletes_received = 0

        reading = False
        update_commands = []

        while True:
            try:
                # Once we have an item sent, we'll read until the end (EOM).
                # If the child dies in the middle of messaging, we get out of the while loop by timeout = 60
                cmd, objstr = self.inventory_update_queue.get(block = reading, timeout = 60)
            except Queue.Empty:
                if reading:
                    # The child process crashed or timed out
                    return 2, update_commands
                else:
                    return 0, update_commands
            else:
                self.inventory_update_queue.task_done()

                reading = True # Now we have to read until the end - start blocking queue.get

                if LOG.getEffectiveLevel() == logging.DEBUG:
                    if cmd == DynamoInventory.CMD_UPDATE:
                        LOG.debug('Update %d from queue: %s', updates_received, objstr)
                    elif cmd == DynamoInventory.CMD_DELETE:
                        LOG.debug('Delete %d from queue: %s', deletes_received, objstr)

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

    def _collect_updates_from_web(self):
        if self.manager.master.get_writing_process_id() != 0 or self.manager.master.get_writing_process_host() != self.manager.hostname:
            return

        read_state, update_commands = self._collect_updates()

        pid = self.manager.master.get_web_write_process_id()

        if read_state == 0:
            LOG.debug('No updates received from the web process.')
            try:
                os.kill(pid, 0) # poll the process
            except OSError:
                # no such process
                LOG.debug('Web process %d has terminated.', pid)
                pass
            else:
                LOG.debug('Web process %d is still running.', pid)
                return

        elif read_state == 1 and len(update_commands) != 0:
            LOG.info('Updating the inventory with data sent from web.')
            self._update_inventory(update_commands)

        LOG.debug('Releasing write lock.')
        self.manager.master.stop_write_web()

    def _cleanup(self):
        # retain_records_for given in days
        cutoff = int(time.time()) - self.applications_config.retain_records_for * 24 * 60 * 60

        applications = self.manager.master.get_applications(older_than = cutoff)

        for app in applications:
            LOG.debug('Cleaning up %s (%s).', app['title'], app['path'])

            if os.path.isdir(app['path']):
                if app['user_host'] != self.manager.hostname:
                    # First make sure all mounts are removed.
                    serverutils.clean_remote_request(app['path'])
    
                # Then remove the path if created by appserver
                if app['path'].startswith(self.appserver.workarea_base):
                    try:
                        shutil.rmtree(app['path'])
                    except OSError:
                        pass

            # Finally remove the entry
            self.manager.master.delete_application(app['appid'])

    def _update_inventory(self, update_commands):
        # My updates
        self.manager.set_status(ServerHost.STAT_UPDATING)

        with SignalBlocker():
            self._exec_updates(update_commands)

        self.manager.set_status(ServerHost.STAT_ONLINE)

        # Others
        self.manager.send_updates(update_commands)

    def _read_updates(self):
        update_commands = self.manager.get_updates()

        num_updates, num_deletes = self._exec_updates(update_commands)

        if num_updates + num_deletes != 0:
            LOG.info('Received %d updates and %d deletes from a remote server.', num_updates, num_deletes)
            # The server which sent the updates has set this server's status to updating
            self.manager.set_status(ServerHost.STAT_ONLINE)

    def _exec_updates(self, update_commands):
        num_updates = 0
        num_deletes = 0
        for cmd, objstr in update_commands:
            # Create a python object from its representation string
            obj = self.inventory.make_object(objstr)

            if cmd == DynamoInventory.CMD_UPDATE:
                num_updates += 1
                embedded_object = self.inventory.update(obj)
                CHANGELOG.info('Saved %s', str(embedded_object))

            elif cmd == DynamoInventory.CMD_DELETE:
                num_deletes += 1
                deleted_object = self.inventory.delete(obj)
                if deleted_object is not None:
                    CHANGELOG.info('Deleting %s', str(deleted_object))

        if num_updates + num_deletes != 0:
            if self.inventory.has_store:
                self.manager.master.advertise_store_version(self.inventory.store_version())

            if self.webserver:
                # Restart the web server so it gets the latest inventory image
                self.webserver.restart()

        return num_updates, num_deletes

    def _start_subprocess(self, app, is_local):
        proc_args = (app['path'], app['args'], is_local, app['auth_level'])

        proc = multiprocessing.Process(target = self.run_script, name = app['title'], args = proc_args)
        proc.daemon = True
        proc.start()

        return proc

    def run_script(self, path, args, is_local, auth_level):
        """
        Main function for script execution.
        @param path            Path to the work area of the script. Will be the root directory in read-only processes.
        @param args            Script command-line arguments.
        @param is_local        True if script is requested from localhost.
        @param defaults_config A Configuration object specifying the global defaults for various tools
        @param auth_level      AppManager.LV_*
        """
    
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        stdout = open(path + '/_stdout', 'a')
        stderr = open(path + '/_stderr', 'a')
        sys.stdout = stdout
        sys.stderr = stderr

        # Create an inventory proxy object used as "the" inventory within the subprocess
        inventory = self.inventory.create_proxy()

        path = self._pre_execution(path, is_local, auth_level, inventory)
    
        # Set argv
        sys.argv = [path + '/exec.py']
        if args:
            sys.argv += shlex.split(args) # split using shell-like syntax
    
        # Execute the script
        try:
            myglobals = {'__builtins__': __builtins__, '__name__': '__main__', '__file__': 'exec.py', '__doc__': None, '__package__': None}
            execfile(path + '/exec.py', myglobals)
    
        except:
            exc_type, exc, tb = sys.exc_info()
    
            if exc_type is SystemExit:
                # sys.exit used in the script
                if exc.code == 0:
                    if auth_level == AppManager.LV_WRITE:
                        self._send_updates(inventory)
                else:
                    raise
    
            elif exc_type is KeyboardInterrupt:
                # Terminated by the server.
                sys.exit(2)
    
            else:
                # print the traceback "manually" to cut out the first two lines showing the server process
                tb_lines = traceback.format_tb(tb)[1:]
                sys.stderr.write('Traceback (most recent call last):\n')
                sys.stderr.write(''.join(tb_lines))
                sys.stderr.write('%s: %s\n' % (exc_type.__name__, str(exc)))
                sys.stderr.flush()
        
                sys.exit(1)
    
        else:
            if auth_level == AppManager.LV_WRITE:
                self._send_updates(inventory)
                # Queue stays available on the other end even if we terminate the process
    
        finally:
            # cleanup
            self._post_execution(path, is_local)
    
            sys.stdout.close()
            sys.stderr.close()
            # multiprocessing/forking.py still uses sys.stdout and sys.stderr - need to return them to the original FDs
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def run_interactive(self, path, is_local, make_console, stdout = sys.stdout, stderr = sys.stderr):
        """
        Main function for interactive sessions.
        For now we limit interactive sessions to read-only.
        @param path            Path to the work area.
        @param is_local        True if script is requested from localhost.
        @param make_console    A callable which takes a dictionary of locals as an argument and returns a console
        @param stdout          File-like object for stdout
        @param stderr          File-like object for stderr
        """
    
        # Create an inventory proxy object used as "the" inventory within the subprocess
        inventory = self.inventory.create_proxy()
    
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout
        sys.stderr = stderr
    
        self._pre_execution(path, is_local, AppManager.LV_NOAUTH, inventory)
    
        # use receive of oconn as input
        mylocals = {'__builtins__': __builtins__, '__name__': '__main__', '__doc__': None, '__package__': None, 'inventory': inventory}
        console = make_console(mylocals)
        try:
            console.interact(serverutils.BANNER)
        finally:
            self._post_execution(path, is_local)
    
        sys.stdout = old_stdout
        sys.stderr = old_stderr

    def _pre_execution(self, path, is_local, auth_level, inventory):
        uid = os.geteuid()
        gid = os.getegid()
    
        # Set defaults
        for key, config in self.defaults_config.items():
            try:
                if auth_level == AppManager.LV_NOAUTH:
                    myconf = config['readonly']
                else:
                    myconf = config['fullauth']
            except KeyError:
                try:
                    myconf = config['all']
                except KeyError:
                    continue
            else:
                try:
                    # security measure
                    del config['fullauth']
                except KeyError:
                    pass
    
            modname, clsname = key.split(':')
            module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
            cls = getattr(module, clsname)
    
            cls.set_default(myconf)
    
        if is_local:
            os.chdir(path)
        else:
            # Confine in a chroot jail
            # Allow access to directories in PYTHONPATH with bind mounts
            for base in find_common_base(map(os.path.realpath, sys.path)):
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
    
        # De-escalate privileges permanently
        os.seteuid(0)
        os.setegid(0)
        os.setgid(gid)
        os.setuid(uid)
    
        # We will react to SIGTERM by raising KeyboardInterrupt
        from dynamo.utils.signaling import SignalConverter
    
        signal_converter = SignalConverter()
        signal_converter.set(signal.SIGTERM)
        # we won't call unset()
    
        # Ignore SIGINT
        # If the main process was interrupted by Ctrl+C:
        # Ctrl+C will pass SIGINT to all child processes (if this process is the head of the
        # foreground process group). In this case calling terminate() will duplicate signals
        # in the child. Child processes have to always ignore SIGINT and be killed only from
        # SIGTERM sent by the line below.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    
        # Reset logging
        reset_logger()
    
        # Pass my inventory and authorizer to the executable through core.executable
        import dynamo.core.executable as executable
        executable.inventory = inventory
        executable.authorizer = self.manager.master.create_authorizer()
    
        if auth_level == AppManager.LV_NOAUTH:
            pass
        else:
            executable.authorized = True
            if auth_level == AppManager.LV_WRITE:
                # create a list of updated and deleted objects the executable can fill
                inventory._update_commands = []
    
        return path

    def _post_execution(self, path, is_local):
        if not is_local:
            # jobs were confined in a chroot jail
            serverutils.clean_remote_request(path)
    
    def _send_updates(self, inventory):
        # Collect updates if write-enabled
    
        nobj = len(inventory._update_commands)

        sys.stderr.write('Sending %d updated objects to the server process.\n' % nobj)
        sys.stderr.flush()

        wm = 0.
        for iobj, (cmd, objstr) in enumerate(inventory._update_commands):
            if float(iobj) / nobj * 100. > wm:
                sys.stderr.write(' %.0f%%..' % (float(iobj) / nobj * 100.))
                sys.stderr.flush()
                wm += 5.
    
            try:
                self.inventory_update_queue.put((cmd, objstr))
            except:
                sys.stderr.write('Exception while sending %s %s\n' % (DynamoInventory._cmd_str[cmd], objstr))
                sys.stderr.flush()
                raise
    
        if nobj != 0:
            sys.stderr.write(' 100%.\n')
            sys.stderr.flush()
        
        # Put end-of-message
        self.inventory_update_queue.put((DynamoInventory.CMD_EOM, None))
    
        # Wait until all messages are received
        self.inventory_update_queue.join()
