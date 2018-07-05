import time
import json
import socket
import MySQLdb

from dynamo.core.components.master import MasterServer
from dynamo.core.components.host import ServerHost, OutOfSyncError
from .mysqlauthorizer import MySQLAuthorizer
from .mysqlappmanager import MySQLAppManager
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class MySQLMasterServer(MySQLAuthorizer, MySQLAppManager, MasterServer):
    def __init__(self, config):
        MySQLAuthorizer.__init__(self, config)
        MySQLAppManager.__init__(self, config)
        MasterServer.__init__(self, config)

        self._host = self._mysql.hostname()

        self._server_id = 0
        
        # we'll be using table locks
        self._mysql.reuse_connection = True

    def _connect(self): #override
        if self._host == 'localhost' or self._host == socket.gethostname():
            # This is the master server; wipe the table clean
            self._mysql.query('DELETE FROM `servers`')
            self._mysql.query('ALTER TABLE `servers` AUTO_INCREMENT = 1')
        else:
            self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', socket.gethostname())

        # id of this server
        self._server_id = self._mysql.insert_get_id('servers', columns = ('hostname', 'last_heartbeat'), values = (socket.gethostname(), MySQL.bare('NOW()')))

    def _do_lock(self): #override
        self._mysql.lock_tables(write = ['servers', 'applications'], read = ['users'])

    def _do_unlock(self): #override
        self._mysql.unlock_tables()

    def get_host(self): #override
        return self._host

    def set_status(self, status, hostname): #override
        self._mysql.query('UPDATE `servers` SET `status` = %s WHERE `hostname` = %s', ServerHost.status_name(status), hostname)

    def get_status(self, hostname): #override
        result = self._mysql.query('SELECT `status` FROM `servers` WHERE `hostname` = %s', hostname)
        if len(result) == 0:
            return None
        else:
            return ServerHost.status_val(result[0])

    def get_host_list(self, status = None, detail = False): #override
        if detail:
            sql = 'SELECT `hostname`, `last_heartbeat`, `status`, `store_host`, `store_module`,'
            sql += ' `shadow_module`, `shadow_config`, `board_module`, `board_config`'
        else:
            sql = 'SELECT `hostname`, `status`, `store_module` IS NOT NULL'

        sql += ' FROM `servers`'
        if status != None:
            sql += ' WHERE `status` = \'%s\'' % ServerHost.status_name(status)

        sql += ' ORDER BY `id`'

        return self._mysql.query(sql)

    def copy(self, remote_master): #override
        # List of servers
        self._mysql.query('DELETE FROM `servers`')
        self._mysql.query('ALTER TABLE `servers` AUTO_INCREMENT = 1')

        all_servers = remote_master.get_host_list(detail = True)
        fields = ('hostname', 'last_heartbeat', 'status', 'store_host', 'store_module', 'shadow_module', 'shadow_config', 'board_module', 'board_config')
        self._mysql.insert_many('servers', fields, None, all_servers, do_update = True)

        # List of users
        self._mysql.query('DELETE FROM `users`')
        self._mysql.query('ALTER TABLE `users` AUTO_INCREMENT = 1')

        all_users = remote_master.list_users()
        fields = ('name', 'email', 'dn')
        self._mysql.insert_many('users', fields, None, all_users, do_update = True)

        # List of roles
        self._mysql.query('DELETE FROM `roles`')
        self._mysql.query('ALTER TABLE `roles` AUTO_INCREMENT = 1')

        all_roles = remote_master.list_roles()
        fields = ('name',)
        self._mysql.insert_many('roles', fields, MySQL.make_tuple, all_roles, do_update = True)

        # List of authorizations
        self._mysql.query('DELETE FROM `user_authorizations`')

        targets = remote_master.list_authorization_targets()
        targets.append(None)
        for target in targets:
            for user, role in remote_master.list_authorized_users(target):
                self.authorize_user(user, role, target)

    def get_next_master(self, current): #override
        self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', current)
        
        # shadow config must be the same as master
        result = self._mysql.query('SELECT `shadow_module`, `shadow_config` FROM `servers` ORDER BY `id` LIMIT 1')
        if len(result) == 0:
            raise RuntimeError('No servers can become master at this moment')

        module, config_str = result[0]
        return module, Configuration(json.loads(config_str))

    def advertise_store(self, module, config): #override
        config = config.clone()
        if config.db_params.host == 'localhost':
            config.db_params.host = socket.gethostname()

        sql = 'UPDATE `servers` SET `store_module` = %s, `store_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self._server_id)

    def advertise_store_version(self, version): #override
        sql = 'UPDATE `servers` SET `store_version` = %s WHERE `id` = %s'
        self._mysql.query(sql, version, self._server_id)

    def get_store_config(self, hostname): #override
        self._mysql.lock_tables(read = ['servers'])

        try:
            while self.get_status(hostname) == ServerHost.STAT_UPDATING:
                # need to get the version of the remote server when it's not updating
                self._mysql.unlock_tables()
                time.sleep(2)
                self._mysql.lock_tables(read = ['servers'])
            
            sql = 'SELECT `store_module`, `store_config`, `store_version` FROM `servers` WHERE `hostname` = %s'
            result = self._mysql.query(sql, hostname)

        finally:
            self._mysql.unlock_tables()

        if len(result) == 0:
            return None

        module, config_str, version = result[0]

        return module, Configuration(json.loads(config_str)), version

    def advertise_shadow(self, module, config): #override
        config = config.clone()
        if config.db_params.host == 'localhost':
            config.db_params.host = socket.gethostname()

        sql = 'UPDATE `servers` SET `shadow_module` = %s, `shadow_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self._server_id)

    def advertise_board(self, module, config): #override
        config = config.clone()
        if config.db_params.host == 'localhost':
            config.db_params.host = socket.gethostname()

        sql = 'UPDATE `servers` SET `board_module` = %s, `board_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self._server_id)

    def get_board_config(self, hostname): #override
        sql = 'SELECT `board_module`, `board_config` FROM `servers` WHERE `hostname` = %s'
        result = self._mysql.query(sql, hostname)
        if len(result) == 0:
            return None

        module, config_str = result[0]

        if config_str is None:
            return None

        return module, Configuration(json.loads(config_str))

    def declare_remote_store(self, hostname): #override
        server_id = self._mysql.query('SELECT `id` FROM `servers` WHERE `hostname` = %s', hostname)[0]
        self._mysql.query('UPDATE `servers` SET `store_host` = %s WHERE `id` = %s', server_id, self._server_id)

    def add_user(self, name, dn, email = None): #override
        sql = 'INSERT INTO `users` (`name`, `email`, `dn`) VALUES (%s, %s, %s)'
        try:
            inserted = self._mysql.query(sql, name, email, dn)
        except:
            return False

        return inserted != 0

    def update_user(self, name, dn = None, email = None): #override
        if dn is None and email is None:
            return

        args = tuple()

        sql = 'UPDATE `users` SET `dn` = '

        if dn is None:
            sql += '`dn`'
        else:
            sql += '%s'
            args += (dn,)

        sql += ', `email` = '

        if email is None:
            sql += '`email`'
        else:
            sql += '%s'
            args += (email,)

        sql += ' WHERE `name` = %s'

        self._mysql.query(sql, *args)

    def delete_user(self, name): #override
        self._mysql.query('DELETE FROM `users` WHERE `name` = %s', name)

    def add_role(self, name): #override
        sql = 'INSERT INTO `roles` (`name`) VALUES (%s)'
        try:
            inserted = self._mysql.query(sql, name)
        except:
            return False

        return inserted != 0

    def authorize_user(self, user, role, target): #override
        if role is None:
            role_id = 0
        else:
            try:
                role_id = self._mysql.query('SELECT `id` FROM `roles` WHERE `name` = %s', role)[0]
            except IndexError:
                raise RuntimeError('Unknown role %s' % role)

        sql = 'INSERT INTO `user_authorizations` (`user_id`, `role_id`, `target`)'
        sql += ' SELECT u.`id`, %s, %s FROM `users` AS u WHERE u.`name` = %s'
        sql += ' ON DUPLICATE KEY UPDATE `user_id` = `user_id`'

        inserted = self._mysql.query(sql, role_id, target, user)
        return inserted != 0

    def revoke_user_authorization(self, user, role, target): #override
        try:
            user_id = self._mysql.query('SELECT `id` FROM `users` WHERE `name` = %s', user)[0]
        except IndexError:
            raise RuntimeError('Unknown user %s' % user)

        if role is None:
            role_id = 0
        else:
            try:
                role_id = self._mysql.query('SELECT `id` FROM `roles` WHERE `name` = %s', role)[0]
            except IndexError:
                raise RuntimeError('Unknown role %s' % role)

        if target is None:
            sql = 'DELETE FROM `user_authorizations` WHERE `user_id` = %s AND `role_id` = %s AND `target` IS NULL'
            deleted = self._mysql.query(sql, user_id, role_id)
        else:
            sql = 'DELETE FROM `user_authorizations` WHERE `user_id` = %s AND `role_id` = %s AND `target` = %s'
            deleted = self._mysql.query(sql, user_id, role_id, target)

        return deleted != 0

    def check_connection(self): #override
        try:
            self._mysql.query('SELECT 1')
        except:
            self.connected = False
            return False

        self.connected = True
        return True

    def send_heartbeat(self): #override
        self._mysql.query('UPDATE `servers` SET `last_heartbeat` = NOW() WHERE `id` = %s', self._server_id)

    def disconnect(self): #override
        self._mysql.query('DELETE FROM `servers` WHERE `id` = %s', self._server_id)
        self._mysql.close()

    def _make_wrapper(self, method): #override
        # Refine _make_wrapper of MasterServer by specifying that the "connection loss" error
        # must be an OperationalError with code 2003.

        def wrapper(*args, **kwd):
            with self._master_server_lock:
                try:
                    return method(*args, **kwd)

                except MySQLdb.OperationalError as ex:
                    if ex.args[0] == 2003:
                        # 2003: Can't connect to server
                        if self._host == 'localhost' or self._host == socket.gethostname():
                            raise

                        self.connected = False
                        raise OutOfSyncError('Lost connection to remote master MySQL')
                    else:
                        raise

        return wrapper
