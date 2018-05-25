import time
import threading
import socket
import hashlib

from dynamo.core.components.master import MasterServer
from dynamo.core.components.board import UpdateBoard

class OutOfSyncError(Exception):
    pass

class ServerHost(object):
    def __init__(self, hostname):
        self.hostname = hostname
        self.status = 'initial'
        self.has_store = False
        self.board = None


class ServerManager(object):
    """
    Manager for the application and updates table and the connections to other servers.
    """

    _server_statuses = ['initial', 'starting', 'online', 'updating', 'outofsync']
    SRV_INITIAL, SRV_STARTING, SRV_ONLINE, SRV_UPDATING, SRV_ERROR, SRV_OUTOFSYNC = range(1, 7)
    _application_statuses = ['new', 'assigned', 'run', 'done', 'notfound', 'authfailed', 'failed', 'killed']
    APP_NEW, APP_ASSIGNED, APP_RUN, APP_DONE, APP_NOTFOUND, APP_AUTHFAILED, APP_FAILED, APP_KILLED = range(1, 9)

    @staticmethod
    def server_status_name(arg):
        try:
            return ServerManager._server_statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def server_status_val(arg):
        try:
            return eval('ServerManager.SRV_' + arg.upper())
        except:
            return arg

    @staticmethod
    def application_status_name(arg):
        try:
            return ServerManager._application_statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def application_status_val(arg):
        try:
            return eval('ServerManager.APP_' + arg.upper())
        except:
            return arg

    def __init__(self, config):
        # Create a master server interface
        self.master = MasterServer.get_instance(config.master.module, config.master.config)
        self.master.connect()
        self.master_host = self.master.get_master_host()

        if self.master_host != 'localhost' and self.master_host != socket.gethostname():
            # Interface to the master server local shadow
            self.shadow = MasterServer.get_instance(config.shadow.module, config.shadow.config)
        else:
            self.shadow = None

        # Interface to the local update board
        self.board = UpdateBoard.get_instance(config.board.module, config.board.config)

        # Interface to other servers {hostname: ServerHost}
        self.other_servers = {}

        # If using a remote store, name of the host
        self.store_host = ''

        self.hostname = socket.gethostname()
        
        self.status = ServerManager.SRV_INITIAL

        # Heartbeat is sent in a separate thread
        self.heartbeat = threading.Thread(target = self.send_heartbeat)
        self.heartbeat.daemon = True
        self.heartbeat.start()

    def set_status(self, status, hostname = None):
        """
        Set status of this or another host.
        """

        if hostname is None:
            # this host
            self.master.lock()
            try:
                if self.get_status() == ServerManager.SRV_OUTOFSYNC:
                    self.status = ServerManager.SRV_OUTOFSYNC
                    raise OutOfSyncError('Server out of sync')
    
                self.master.set_status(status, self.hostname)

            finally:
                self.master.unlock()

            self.status = status

        else:
            # another host
            self.master.set_status(status, hostname)

    def reset_status(self):
        """
        Reset server status to initial when it is out of sync.
        """
        self.master.lock()
        try:
            # first check that we are out of sync
            if self.get_status() != ServerManager.SRV_OUTOFSYNC:
                raise RuntimeError('reset_status called when status is not outofsync')

            # then reset
            self.master.set_status(ServerManager.SRV_INITIAL, self.hostname)

        finally:
            self.master.unlock()

        self.status = ServerManager.SRV_INITIAL

    def check_status(self):
        """
        1. Check connection to the master server
        2. Update the status from the master server
        """

        if not self.master.check_connection():
            raise RuntimeError('Lost connection to master server')

        self.get_status()

        if self.status == ServerManager.SRV_ERROR:
            raise RuntimeError('Server status is ERROR')

        elif self.status == ServerManager.SRV_OUTOFSYNC:
            raise OutOfSyncError('Server out of sync')

    def get_status(self, hostname = None):
        """
        Read the server status from the master list.
        """
        if hostname is None:
            status = self.master.get_status(self.hostname)
        else:
            status = self.master.get_status(hostname)

        if status is None:
            status_val = None
        else:
            status_val = ServerManager.server_status_val(status)

        if hostname is None:
            self.status = status_val

        return status_val

    def count_servers(self, status):
        """
        Count the number of servers (including self) in the given status.
        @parameter status  A single SRV_* value or a list of SRV_*
        """
        try:
            len(status)
        except TypeError:
            # single value
            return len(self.master.get_host_list(status = status))
        else:
            # use a set just in case for some reason the host changes the status in the middle of counting
            hostnames = set()
            for stat in status:
                hostnames.update(n for n, _, _ in self.master.get_host_list(status = stat))
            return len(hostnames)

    def get_updates(self):
        """
        Return entries from the local update board as an iterable.
        """
        self.board.lock()
        try:
            update_commands = self.board.get_updates()
            has_updates = False
            for cmd, obj in update_commands:
                has_updates = True
                yield (cmd, obj)

            if has_updates:
                self.board.flush()

        finally:
            self.board.unlock()

        return

    def send_heartbeat(self):
        """
        Send the heartbeat to the master server. Additionally check for status updates made by peers.
        If a peer tries to update the inventory content of this server and fails for some reason, the
        server status is forced to be out-of-sync.
        """

        while True:
            if self.status != ServerManager.SRV_INITIAL:
                self.master.send_heartbeat()

                if self.shadow is not None:
                    self.shadow.copy(self.master)
    
            time.sleep(30)

    def reconnect_master(self):
        """
        Find and connect to the new master server.
        """
        if not self.shadow:
            # Master server was local
            raise RuntimeError('Cannot reconnect to local master server.')

        module, config = self.shadow.get_next_master(self.master_host)
        
        self.master = MasterServer.get_instance(module, config)
        self.master.connect()
        self.master_host = self.master.get_master_host()

        if self.master_host == 'localhost' or self.master_host == socket.gethostname():
            self.shadow = None

    def get_next_application(self):
        """
        Fetch the next application to run.
        @return id, write_request, title, path, args, user
        """
        self.master.lock()
        try:
            self.get_status()
            
            # Cannot run a write process if
            #  . I am supposed to be updating my inventory
            #  . There is a server starting
            #  . There is already a write process
            read_only = (len(self.master.get_host_list(status = ServerManager.SRV_STARTING)) != 0) or \
                        (self.master.get_writing_process_id() is not None)

            app = self.master.get_next_application(read_only)
    
            if app is None:
                return None
            else:
                self.master.update_application(app['appid'], status = ServerManager.APP_ASSIGNED, hostname = self.hostname)
                return app

        finally:
            self.master.unlock()

    def get_application_status(self, app_id):
        """
        Get the application status.
        """
        applications = self.master.get_applications(app_id = app_id)
        if len(applications) == 0:
            # We assume the application was killed an removed
            return ServerManager.EXC_KILLED
        else:
            return applications[0]['status']

    def set_application_status(self, app_id, status):
        """
        Set the application status.
        """
        self.master.update_application(app_id, status = status)

    def check_write_auth(self, title, user, path, exc_name = 'exec.py'):
        """
        Check the authorization of write-requesting application. The title, user_id, and the md5 hash of the application
        script must match the registration.

        @param title    Title of the application
        @param user     Requester user name
        @param path     Application path
        @param exc_name Executable file name

        @return boolean
        """
        # check authorization
        with open(path + '/' + exc_name) as source:
            checksum = hashlib.md5(source.read()).hexdigest()

        return self.master.check_application_auth(title, user, checksum)

    def find_remote_store(self, hostname = ''):
        """
        Find a remote host backed up by a persistency store and has status 'online'.
        return None if no server is found, or a pair (module_name, config) if found.
        """
        while True:
            self.collect_hosts()

            is_updating = False
            for server in self.other_servers.itervalues():
                if hostname and server.hostname != hostname:
                    continue

                if not server.has_store:
                    continue

                if server.status == ServerManager.SRV_ONLINE:
                    # store_config = (module, config, version)
                    store_config = self.master.get_store_config(server.hostname)
                    if store_config is None:
                        continue

                    return (server.hostname,) + store_config
                    
                elif server.status == ServerManager.SRV_UPDATING:
                    is_updating = True

            if is_updating:
                time.sleep(5)
            else:
                self.set_status(ServerManager.SRV_ERROR)
                raise RuntimeError('Could not find a remote persistency store to connect to.')

    def register_remote_store(self, hostname):
        self.store_host = hostname
        self.master.declare_remote_store(hostname)

    def collect_hosts(self):
        """
        Keep the other host list up-to-date.
        """
        known_hosts = set()

        for hostname, status, has_store in self.master.get_host_list():
            if hostname == self.hostname:
                continue

            try:
                server = self.other_servers[hostname]
            except KeyError:
                board_conf = self.master.get_board_config(hostname)
                if board_conf is None:
                    # shouldn't happen
                    continue

                server = ServerHost(hostname)
                server.board = UpdateBoard.get_instance(board_conf[0], board_conf[1])

                self.other_servers[hostname] = server

            server.has_store = (has_store != 0)
            server.status = ServerManager.server_status_val(status)

            known_hosts.add(hostname)

        for hostname in set(self.other_servers.keys()) - known_hosts:
            self.other_servers.pop(hostname)

    def send_updates(self, update_commands):
        """
        Send the list of update commands to all online servers.

        @param update_commands  List of two-tuples (cmd, obj)
        """
        # Write-enabled process and server start do not happen simultaneously.
        # No servers could have come online while we were running a write-enabled process - other_servers is the full list
        # of running servers.

        processed = set()

        while True:
            self.master.lock()

            try:
                self.collect_hosts()
    
                # update only one server at a time to minimize the time in lock
                for server in self.other_servers.itervalues():
                    if server.hostname not in processed:
                        break
                else:
                    # all processed, we are done
                    break

                if server.status == ServerManager.SRV_ONLINE:
                    processed.add(server.hostname)

                    self.set_status(ServerManager.SRV_UPDATING, server.hostname)
                    try:
                        server.board.write_updates(update_commands)
                    except:
                        self.set_status(ServerManager.SRV_OUTOFSYNC, server.hostname)

                elif server.status == ServerManager.SRV_UPDATING:
                    # this server is still processing updates from the previous write process
                    continue

                else:
                    # any other status means the server is not running
                    processed.add(server.hostname)

            finally:
                self.master.unlock()

            time.sleep(1)

    def disconnect(self):
        """
        Go offline and delete the entry from the master server list.
        """
        self.master.disconnect()
        for server in self.other_servers.itervalues():
            if server.board:
                server.board.disconnect()
