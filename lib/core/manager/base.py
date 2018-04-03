import threading

class OutOfSyncError(Exception):
    pass

class ServerHost(object):
    def __init__(self, hostname):
        self.hostname = hostname
        self.status = 'initial'
        self.has_store = False
        self.interface = None


class ServerManager(object):
    """
    Manager for the executable and updates table and the connections to other servers.
    """

    _server_statuses = ['initial', 'starting', 'online', 'updating', 'outofsync']
    SRV_INITIAL, SRV_STARTING, SRV_ONLINE, SRV_UPDATING, SRV_ERROR, SRV_OUTOFSYNC = range(1, 7)
    _executable_statuses = ['new', 'assigned', 'run', 'done', 'notfound', 'authfailed', 'failed', 'killed']
    EXC_NEW, EXC_ASSIGNED, EXC_RUN, EXC_DONE, EXC_NOTFOUND, EXC_AUTHFAILED, EXC_FAILED, EXC_KILLED = range(1, 9)

    @staticmethod
    def server_status_name(arg):
        if type(arg) is int:
            return _server_statuses[arg - 1]
        else:
            return arg

    @staticmethod
    def server_status_val(arg):
        if type(arg) is str:
            return eval('ServerManager.SRV_' + arg.upper())
        else:
            return arg

    @staticmethod
    def executable_status_name(arg):
        if type(arg) is int:
            return _executable_statuses[arg - 1]
        else:
            return arg

    @staticmethod
    def executable_status_val(arg):
        if type(arg) is str:
            return eval('ServerManager.EXC_' + arg.upper())
        else:
            return arg

    def __init__(self, config):
        # Interface to the master server
        self.master_server = None
        # Interface to other servers {hostname: ServerHost}
        self.other_servers = {}
        # Is this server backed up by a local persistency store?
        self.has_store = config.has_store
        # If using a remote store, name of the host
        self.store_host = ''
        
        self.status = ServerManager.SRV_INITIAL

        # Heartbeat is sent in a separate thread
        self.heartbeat = threading.Thread(target = self.send_heartbeat)
        self.heartbeat.daemon = True
        self.heartbeat.start()

    def set_status(self, status, hostname = None):
        """
        Write the server status to the master server list and set status.
        """
        self._send_status_to_master(status, hostname)
        self.status = status

    def check_status(self):
        """
        Check server status - other servers may decide this one has gone out of sync
        """
        if self.status == ServerManager.SRV_ERROR:
            raise RuntimeError('Server status is ERROR')

        if self.get_status() == ServerManager.SRV_OUTOFSYNC:
            self.status = ServerManager.SRV_OUTOFSYNC
            raise OutOfSyncError('Server out of sync')

    def get_status(self):
        """
        Read the server status from the master list.
        """
        raise NotImplementedError('get_status')

    def count_servers(self, status):
        """
        Count the number of servers (including self) in the given status.
        """
        raise NotImplementedError('count_servers')

    def get_updates(self):
        """
        Return entries from the local update board as an iterable.
        """
        raise NotImplementedError('get_updates')

    def send_heartbeat(self):
        """
        Send the heartbeat to the master server. Additionally check for status updates made by peers.
        If a peer tries to update the inventory content of this server and fails for some reason, the
        server status is forced to be out-of-sync.
        """

        if self.status != ServerManager.SRV_INITIAL:
            self._send_heartbeat_to_master()

        time.sleep(60)

    def writing_process_id(self):
        """
        Get the id of the writing executable.
        """
        raise NotImplementedError('writing_process_id')

    def get_next_executable(self):
        """
        Fetch the next executable to run.
        """
        raise NotImplementedError('get_next_executable')

    def get_executable_status(self, excec_id):
        """
        Get the executable status.
        """
        raise NotImplementedError('get_executable_status')

    def set_executable_status(self, exec_id, status, exit_code = None):
        """
        Set the executable status.
        
        @param exec_id   Executable id
        @param status    New status
        @param exit_code Exit code (optional)
        """
        raise NotImplementedError('set_executable_status')

    def check_write_auth(self, title, user, path):
        """
        Check the authorization of write-requesting executable. The title, user_id, and the md5 hash of the executable
        script must match the registration.

        @param title   Title of the executable
        @param user    Requester user name
        @param path    Executable path
        """
        raise NotImplementedError('check_write_auth')

    def advertise_store(self, store_config):
        """
        Advertise the configuration of the local persistency store through the master server.
        """
        raise NotImplementedError('advertise_store')

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
                    module, config = self._get_store_config(server.hostname)
                    self.store_host = server.hostname
                    self._set_remote_store()
                    return module, config
                    
                elif server.status == ServerManager.SRV_UPDATING:
                    is_updating = True

            if is_updating:
                time.sleep(5)
            else:
                return None

    def collect_hosts(self):
        """
        Keep the other host list up-to-date.
        """
        raise NotImplementedError('collect_hosts')

    def send_updates(self, update_commands):
        """
        Send the list of update commands to all online servers.

        @param update_commands  List of two-tuples (cmd, obj)
        """
        raise NotImplementedError('send_updates')

    def disconnect(self):
        """
        Go offline and delete the entry from the master server list.
        """
        raise NotImplementedError('disconnect')
