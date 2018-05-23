import os
import sys
import shutil
import time
import logging
import socket
import signal
import code
import multiprocessing
import threading
import Queue

from dynamo.core.inventory import DynamoInventory
from dynamo.core.manager import ServerManager, OutOfSyncError
import dynamo.core.serverutils as serverutils
from dynamo.core.components.appserver import AppServer
from dynamo.utils.log import log_exception

LOG = logging.getLogger(__name__)
CHANGELOG = logging.getLogger('changelog')

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
        if self.applications_config.enabled:
            # Initialize the appserver since it may require elevated privilege (this Ctor is run as root)
            aconf = self.applications_config.server
            self.appserver = AppServer.get_instance(aconf.module, self, aconf.config)

            if self.applications_config.timeout < 60:
                # Some errors were observed when the timeout is too short
                # (probably 1 second is enough - we just need to get through pre_execution)
                self.applications_config.timeout = 60

        ## Server status (and application) poll interval
        self.poll_interval = config.status_poll_interval

        ## Load the inventory content (filter according to debug config)
        self.inventory_load_opts = {}
        if 'debug' in config:
            for objs in ['groups', 'sites', 'datasets']:
                included = config.debug.get('included_' + objs, None)
                excluded = config.debug.get('excluded_' + objs, None)
    
                self.inventory_load_opts[objs] = (included, excluded)

        ## Shutdown flag
        # Default is set. KeyboardInterrupt is raised when flag is cleared
        self.shutdown_flag = threading.Event()
        self.shutdown_flag.set()

    def load_inventory(self):
        self.inventory = DynamoInventory(self.inventory_config)

        ## Wait until there is no write process
        while self.manager.master.get_writing_process_id() is not None:
            LOG.debug('A write-enabled process is running. Checking again in 5 seconds.')
            time.sleep(5)

        ## Write process is done.
        ## Other servers will not start a new write process while there is a server with status 'starting'.
        ## The only states the other running servers can be in are therefore 'updating' or 'online'
        while self.manager.count_servers(ServerManager.SRV_UPDATING) != 0:
            time.sleep(2)

        if self.manager.count_servers(ServerManager.SRV_ONLINE) == 0:
            # I am the first server to start the inventory - need to have a store.
            if not self.inventory.has_store:
                raise RuntimeError('No persistent inventory storage is available.')
        else:
            if self.inventory.has_store:
                # Clone the content from a remote store
                hostname, module, config, version = self.manager.find_remote_store()
                # No server will be updating because write process is blocked while we load
                if version == self.inventory.store_version():
                    LOG.info('Local persistency store is up to date.')
                else:
                    # TODO cloning can take hours; need a way to unblock other servers and pool the updates
                    LOG.info('Cloning inventory content from persistency store at %s', hostname)
                    self.inventory.clone_store(module, config)
            else:
                self._setup_remote_store()

        LOG.info('Loading the inventory.')
        self.inventory.load(**self.inventory_load_opts)

        LOG.info('Inventory is ready.')

    def run(self):
        """
        Main body of the server, but mostly focuses on exception handling. dynamod runs this function
        in a non-main thread.
        """

        # Outer loop: restart the application server when the inventory goes out of synch
        while True:
            # Lock write activities by other servers
            self.manager.set_status(ServerManager.SRV_STARTING)

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
            self.manager.set_status(ServerManager.SRV_ONLINE)

            try:
                # Actual stuff happens here
                # Both functions are infinite loops; the only way out is an exception (can be a peaceful KeyboardInterrupt)
                if self.applications_config.enabled:
                    self._run_application_cycles()
                else:
                    self._run_update_cycles()

            except KeyboardInterrupt:
                LOG.info('Server process was interrupted.')
                # KeyboardInterrupt is raised when shutdown_flag is set
                # Notify shutdown ready
                self.shutdown_flag.set()

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
                    self.manager.status = ServerManager.SRV_INITIAL
   
            except:
                log_exception(LOG)
                LOG.error('Shutting down Dynamo.')
                # Call sigint on myself to get out of the web server / signal.pause()
                os.kill(os.getpid(), signal.SIGINT)
                # Once this function returns, dynamod calls server.shutdown()
                # If the flag is not cleared already, shutdown() will wait 60 seconds before timing out.
                self.shutdown_flag.clear()

                break

    def check_status_and_connection(self):
        if not self.shutdown_flag.is_set():
            raise KeyboardInterrupt('Shutdown')

        ## Check status (raises exception if error)
        self.manager.check_status()
    
        if not self.inventory.check_store():
            # We lost connection to the remote persistency store. Try another server.
            # If there is no server to connect to, this method raises a RuntimeError
            self._setup_remote_store()

    def shutdown(self):
        # Called by dynamod
        # Clears the flag so that check_status_and_connection raises a KeyboardInterrupt
        LOG.info('Shutting down Dynamo server..')

        if self.shutdown_flag.is_set():
            self.shutdown_flag.clear()
            state = self.shutdown_flag.wait(60)
            if not state:
                # timed out
                LOG.warning('Shutdown timeout of 60 seconds have passed.')

        self.manager.disconnect()

    def get_subprocess_args(self):
        # Create a inventory proxy with a fresh connection to the store backend
        # Otherwise my connection will be closed when the inventory is garbage-collected in the child process
        inventory_proxy = self.inventory.create_proxy()

        # Similarly pass a new Authorizer with a fresh connection
        authorizer = self.manager.master.create_authorizer()

        return self.applications_config.defaults, inventory_proxy, authorizer

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
            # There can only be one child process with write access at a time. We pass it a Queue to communicate back.
            # writing_process is a tuple (proc, queue) when some process is writing
            writing_process = (0, None)
    
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
                writing_process = self._collect_processes(child_processes, writing_process)

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
    
                app = self.manager.get_next_application()
    
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
                    LOG.info('Application %s from %s@%s (write request: %s) not found.', app['title'], app['user_name'], app['user_host'], app['write_request'])
                    self.manager.master.update_application(app['appid'], status = ServerManager.APP_NOTFOUND)
                    self.appserver.notify_synch_app(app['appid'], {'status': ServerManager.APP_NOTFOUND})
                    continue
    
                LOG.info('Found application %s from %s (write request: %s)', app['title'], app['user_name'], app['write_request'])

                is_local = (app['user_host'] == socket.gethostname())
    
                if app['write_request']:
                    if not self.manager.check_write_auth(app['title'], app['user_name'], app['path']):
                        LOG.warning('Application %s from %s is not authorized for write access.', app['title'], app['user_name'])
                        # TODO send a message
    
                        self.manager.master.update_application(app['appid'], status = ServerManager.APP_AUTHFAILED)
                        self.appserver.notify_synch_app(app['appid'], {'status': ServerManager.APP_AUTHFAILED})
                        continue
    
                    queue = multiprocessing.Queue()
                    writing_process = (app['appid'], queue)
                else:
                    queue = None
    
                ## Step 3: Spawn a child process for the script
                self.manager.master.update_application(app['appid'], status = ServerManager.APP_RUN)
    
                proc = self._start_subprocess(app, is_local, queue)
                
                self.appserver.notify_synch_app(app['appid'], {'status': ServerManager.APP_RUN, 'path': app['path'], 'pid': proc.pid})
    
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

            for app_id, proc, user_name, user_host, path, time_start in child_processes:
                LOG.warning('Terminating %s (%s) from %s@%s (AID %d PID %d)', proc.name, path, user_name, user_host, app_id, proc.pid)

                serverutils.killproc(proc, LOG)

                try:
                    self.manager.master.update_application(app_id, status = ServerManager.APP_KILLED)
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

    def _setup_remote_store(self, hostname = ''):
        # find_remote_store raises a RuntimeError if not source is found
        hostname, module, config, version = self.manager.find_remote_store(hostname = hostname)
        LOG.info('Using persistency store at %s', hostname)
        self.manager.register_remote_store(hostname)

        self.inventory.init_store(module, config)

    def _collect_processes(self, child_processes, writing_process):
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

        ichild = 0
        while ichild != len(child_processes):
            app_id, proc, user_name, user_host, path, time_start = child_processes[ichild]

            id_str = '%s (%s) from %s@%s (AID %d PID %d)' % (proc.name, path, user_name, user_host, app_id, proc.pid)

            apps = self.manager.master.get_applications(app_id = app_id)
            if len(apps) == 0:
                status = ServerManager.APP_KILLED
            else:
                status = apps[0]['status']

            # Kill processes running for too long (timeout given in seconds)
            if time_start < time.time() - self.applications_config.timeout:
                LOG.warning('Application %s timed out.', id_str)
                status = ServerManager.APP_KILLED

            if app_id == writing_process[0]:
                # If this is the writing process, read data from the queue
                # read_state: 0 -> nothing written yet (process is running), 1 -> read OK, 2 -> failure
                read_state, update_commands = self._collect_updates(writing_process[1])

                if status == ServerManager.APP_RUN:
                    if read_state == 1:
                        # we would block signal here, but since we would be running this code in a subthread we don't have to
                        self._update_inventory(update_commands)
    
                    elif read_state == 2:
                        status = ServerManager.APP_FAILED
                        serverutils.killproc(proc, LOG, 60)

                # If the process is killed or updates are read, release the writing_process
                if status != ServerManager.APP_RUN or read_state != 0:
                    writing_process = (0, None)

            if status == ServerManager.APP_KILLED and proc.is_alive():
                LOG.warning('Terminating %s.', id_str)
                serverutils.killproc(proc, LOG, 60)

            if proc.is_alive():
                if status == ServerManager.APP_RUN:
                    ichild += 1
                    continue
                else:
                    # The process must be complete but did not join within 60 seconds
                    LOG.error('Application %s is stuck (Status %s).', id_str, ServerManager.application_status_name(status))
            else:
                if status == ServerManager.APP_RUN:
                    if proc.exitcode == 0:
                        status = ServerManager.APP_DONE
                    else:
                        status = ServerManager.APP_FAILED

                LOG.info('Application %s completed (Exit code %d Status %s).', id_str, proc.exitcode, ServerManager.application_status_name(status))
               
            child_processes.pop(ichild)

            self.appserver.notify_synch_app(app_id, {'status': status, 'exit_code': proc.exitcode})

            self.manager.master.update_application(app_id, status = status, exit_code = proc.exitcode)

        return writing_process

    def _collect_updates(self, queue):
        print_every = 100000
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

    def _cleanup(self):
        # retain_records_for given in days
        cutoff = int(time.time()) - self.applications_config.retain_records_for * 24 * 60 * 60

        applications = self.manager.master.get_applications(older_than = cutoff)

        for app in applications:
            LOG.debug('Cleaning up %s (%s).', app['title'], app['path'])

            if os.path.isdir(app['path']):
                if app['user_host'] != socket.gethostname():
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
        self.manager.set_status(ServerManager.SRV_UPDATING)

        self._exec_updates(update_commands)

        self.manager.set_status(ServerManager.SRV_ONLINE)

        # Others
        self.manager.send_updates(update_commands)

    def _read_updates(self):
        update_commands = self.manager.get_updates()

        has_update = self._exec_updates(update_commands)

        # update_commands is an iterator - cannot just do len()
        if has_update:
            # The server which sent the updates has set this server's status to updating
            self.manager.set_status(ServerManager.SRV_ONLINE)

    def _exec_updates(self, update_commands):
        has_update = False
        for cmd, objstr in update_commands:
            has_update = True
            # Create a python object from its representation string
            obj = self.inventory.make_object(objstr)

            if cmd == DynamoInventory.CMD_UPDATE:
                embedded_object = self.inventory.update(obj)
                CHANGELOG.info('Saved %s', str(embedded_object))

            elif cmd == DynamoInventory.CMD_DELETE:
                deleted_object = self.inventory.delete(obj)
                if deleted_object is not None:
                    CHANGELOG.info('Deleting %s', str(deleted_object))

        if has_update and self.inventory.has_store:
            self.manager.master.advertise_store_version(self.inventory.store_version())

        return has_update

    def _start_subprocess(self, app, is_local, queue):
        defaults_conf, inventory, authorizer = self.get_subprocess_args()

        proc_args = (app['path'], app['args'], is_local, defaults_conf, inventory, authorizer, queue)

        proc = multiprocessing.Process(target = serverutils.run_script, name = app['title'], args = proc_args)
        proc.daemon = True
        proc.start()

        return proc
