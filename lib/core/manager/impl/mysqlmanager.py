import time
import socket
import logging
import json

from dynamo.core.manager.base import ServerManager, ServerHost, OutOfSyncError
from dynamo.core.inventory import DynamoInventory
from dynamo.dataformat import Configuration
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class MySQLServerManager(ServerManager):
    """Implementation of ServerManager using MySQL."""

    def __init__(self, config):
        ServerManager.__init__(self, config)

        self.master_server = MySQL(config.master_server)
        self.master_server.query('LOCK TABLES `servers` WRITE')

        self.master_server_config = config.master_server.clone()

        try:
            self.master_server.query('DELETE FROM `servers` WHERE `hostname` = %s', socket.gethostname())
            self.server_id = self.master_server.query('INSERT INTO `servers` (`hostname`, `last_heartbeat`) VALUES (%s, NOW())', socket.gethostname())

            self.collect_hosts()
    
        finally:
            self.master_server.query('UNLOCK TABLES')

        host_conf = config.master_server.clone()
        host_conf.host = 'localhost'
        self.local_db = MySQL(host_conf)

    def get_status(self, hostname = None): #override
        if hostname is None:
            status = self.master_server.query('SELECT `status` FROM `servers` WHERE `id` = %s', self.server_id)[0]
        else:
            result = self.master_server.query('SELECT `status` FROM `servers` WHERE `hostname` = %s', hostname)
            if len(result) == 0:
                return None

            status = result[0]
            
        return ServerManager.server_status_val(status)

    def count_servers(self, status): #override
        return self.master_server.query('COUNT (*) FROM `servers` WHERE `status` = %s', ServerManager.server_status_name(status))[0]

    def get_updates(self): #override
        # Read updates written to the registry by other servers
        self.local_db.query('LOCK TABLES `inventory_updates` WRITE')

        try:
            for cmd, obj in self.local_db.xquery('SELECT `cmd`, `obj` FROM `inventory_updates` ORDER BY `id`'):
                yield (cmd, obj)
    
            self.local_db.query('ALTER TABLE `inventory_updates` AUTO_INCREMENT = 1')

        finally:
            self.local_db.query('UNLOCK TABLES')

        return

    def writing_process_id(self): #override
        result = self.master_server.query('SELECT `id` FROM `executables` WHERE `write_request` = 1 AND `status` = \'run\'')
        if len(result) == 0:
            return None
        else:
            return result[0]

    def schedule_executable(self, title, path, args, user, write_request): #override
        sql = 'INSERT INTO `executables` (`write_request`, `title`, `path`, `args`, `user`) VALUES (%s, %s, %s, %s, %s)'
        return self.master_server.query(sql, write_request, title, path, args, user)

    def get_next_executable(self): #override
        self.master_server.query('LOCK TABLES `servers` READ, `executable` WRITE')
        
        try:
            # Cannot run a write process if
            #  . I am supposed to be updating my inventory
            #  . There is a server starting
            #  . There is already a write process
            skip_writer = (self.get_status() == ServerManager.SRV_UPDATING)
            if not skip_writer:
                skip_writer = (self.count_servers(ServerManager.SRV_STARTING) != 0)
                if not skip_writer:
                    num_running_writes = self.master_server.query('COUNT (*) FROM `executables` WHERE `write_request` = 1 AND `status` = \'run\'')[0]
                    skip_writer = (num_running_writes != 0)
    
            sql = 'SELECT `id`, `write_request`, `title`, `path`, `args`, `user` FROM `executables`'
            sql += ' WHERE `status` = \'new\''
            if skip_writer:
                sql += ' AND `write_request` = 0'
            sql += ' ORDER BY `timestamp` LIMIT 1'
            result = self.master_server.query(sql)
    
            if len(result) == 0:
                return None
            else:
                sql = 'UPDATE `executables` SET `status` = \'assigned\', `server_id` = %s WHERE `id` = %s'
                self.master_server.query(sql, self.server_id, result[0][0])
                return result[0]

        finally:
            self.master_server.query('UNLOCK TABLES')

    def get_executable_status(self, excec_id): #override
        result = self.master_server.query('SELECT `status` FROM `action` WHERE `id` = %s', exec_id)
        if len(result) == 0:
            # don't know what happened but the executable is gone
            return ServerManager.EXC_KILLED
        else:
            return ServerManager.executable_status_val(result[0])

    def set_executable_status(self, exec_id, status, exit_code = None): #override
        self.master_server.query('UPDATE `executables` SET `status` = %s, `exit_code` = %s WHERE `id` = %s', ServerManager.executable_status_name(status), exit_code, exec_id)

    def check_write_auth(self, title, user, path): #override
        # check authorization
        with open(path + '/exec.py') as source:
            checksum = hashlib.md5(source.read()).hexdigest()

        sql = 'SELECT `user_id` FROM `authorized_executables` WHERE `title` = %s AND `checksum` = UNHEX(%s)'
        for auth_user_id in self.registry.backend.query(sql, title, checksum):
            if auth_user_id == 0 or auth_user_id == user_id:
                return True

        return False

    def advertise_store(self, store_config): #override
        sql = 'UPDATE `servers` SET `store_module` = %s, `store_config` = %s WHERE `id` = %s'
        self.master_server.query(sql, store_config.module, store_config.config.dump_json(), self.server_id)

    def collect_hosts(self): #override
        known_hosts = set()

        for hostname, status, has_store in self.master_server.query('SELECT `hostname`, `status`, `store_module` IS NOT NULL FROM `servers` WHERE `id` != %s', self.server_id):
            try:
                host = self.other_servers[hostname]
            except KeyError:
                host = ServerHost(hostname)

                host_conf = self.master_server_config.clone()
                host_conf.host = hostname
                host.interface = MySQL(host_conf)

                self.other_servers[hostname] = host

            host.has_store = (has_store != 0)
            host.status = ServerManager.server_status_val(status)

            known_hosts.add(hostname)

        for hostname in set(self.other_servers.keys()) - known_hosts:
            self.other_servers.pop(hostname)

    def disconnect(self): #override
        self.master_server.query('DELETE FROM `servers` WHERE `id` = %s', self.server_id)

    def _send_status_to_master(self, status, hostname): #override
        if hostname is None:
            self.master_server.query('LOCK TABLES `servers` WRITE')

            try:
                if self.get_status() == ServerManager.SRV_OUTOFSYNC:
                    self.status = ServerManager.SRV_OUTOFSYNC
                    raise OutOfSyncError('Server out of sync')
    
                self.master_server.query('UPDATE `servers` SET `status` = %s WHERE `id` = %s', ServerManager.executable_status_name(status), self.server_id)

            finally:
                self.master_server.query('UNLOCK TABLES')

        else:
            self.master_server.query('UPDATE `servers` SET `status` = %s WHERE `hostname` = %s', ServerManager.executable_status_name(status), hostname)

    def _send_heartbeat_to_master(self): #override
        self.master_server.query('UPDATE `servers` SET `last_heartbeat` = NOW() WHERE `id` = %s', self.server_id)

    def send_updates(self, update_commands): #override
        # Write-enabled process and server start do not happen simultaneously.
        # No servers could have come online while we were running a write-enabled process - other_servers is the full list
        # of running servers.

        processed = set()

        while True:
            self.master_server.query('LOCK TABLES `servers` WRITE')

            try:
                self.collect_hosts() # uses only `servers`
    
                # update only one server at a time to minimize the time in lock
                for server in self.other_servers.itervalues():
                    if server.hostname not in processed:
                        break
                else:
                    # all processed, we are done
                    break

                if server.status == ServerManager.SRV_ONLINE:
                    processed.add(server.hostname)
                    self._send_updates_to_server(server, update_commands)

                elif server.status == ServerManager.SRV_UPDATING:
                    # this server is still processing updates from the previous write process
                    continue

                else:
                    # any other status means the server is not running
                    processed.add(server.hostname)

            finally:
                self.master_server.query('UNLOCK TABLES')

            time.sleep(1)

    def _send_updates_to_server(self, server, update_commands):
        server.interface.query('LOCK TABLES `inventory_updates` WRITE')

        self.set_status(ServerManager.SRV_UPDATING, server.hostname)

        try:
            sql = 'INSERT INTO `inventory_updates` (`cmd`, `obj`) VALUES (%s, %s)'

            for cmd, sobj in update_commands:
                if cmd == DynamoInventory.CMD_UPDATE:
                    server.interface.query(sql, 'update', sobj)
                elif cmd == DynamoInventory.CMD_DELETE:
                    server.interface.query(sql, 'delete', sobj)
        except:
            # TODO print error
            # Set server status to out-of-sync
            self.set_status(ServerManager.SRV_OUTOFSYNC, server.hostname)

        finally:
            server.interface.query('UNLOCK TABLES')

    def _get_store_config(self, hostname): #override
        sql = 'SELECT `store_module`, `store_config` FROM `servers` WHERE `hostname` = %s'
        result = self.master_server.query(sql, hostname)
        if len(result) == 0:
            raise RuntimeError('Cannot find store config for ' + hostname)

        module, config_str = result[0]

        return module, Configuration(json.loads(config_str))

    def _set_remote_store(self): #override
        server_id = self.master_server.query('SELECT `id` FROM `servers` WHERE `hostname` = %s', self.store_host)
        self.master_server.query('UPDATE `servers` SET `store_host` = %s', server_id)
