import time
import json
import socket

from dynamo.core.components.master import Authorizer, AppManager, MasterServer
from dynamo.core.manager import ServerManager
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class MySQLAuthorizer(Authorizer):
    def __init__(self, config):
        Authorizer.__init__(self, config)

        if not hasattr(self, '_mysql'):
            db_params = Configuration(config.db_params)
            db_params.reuse_connection = True # we use locks
    
            self._mysql = MySQL(db_params)

    def user_exists(self, name):
        result = self._mysql.query('SELECT COUNT(*) FROM `users` WHERE `name` = %s', name)[0]
        return result != 0

    def list_users(self):
        return self._mysql.query('SELECT `name`, `email`, `dn` FROM `users` ORDER BY `id`')

    def identify_user(self, dn = '', name = '', with_id = False): #override
        if dn:
            result = self._mysql.query('SELECT `name`, `id` FROM `users` WHERE `dn` = %s', dn)
        else:
            result = self._mysql.query('SELECT `name`, `id` FROM `users` WHERE `name` = %s', name)

        if len(result) == 0:
            return None
        else:
            if with_id:
                return (result[0][0], int(result[0][1]))
            else:
                return result[0][0]

    def identify_role(self, name, with_id = False): #override
        try:
            if with_id:
                name, rid = self._mysql.query('SELECT `name`, `id` FROM `roles` WHERE `name` = %s', name)[0]
                return (name, int(rid))
            else:
                return self._mysql.query('SELECT `name` FROM `roles` WHERE `name` = %s', name)[0]
        except IndexError:
            return None

    def list_roles(self):
        return self._mysql.query('SELECT `name` FROM `roles`')

    def list_authorization_targets(self): #override
        sql = 'SELECT SUBSTRING(COLUMN_TYPE, 5) FROM `information_schema`.`COLUMNS`'
        sql += ' WHERE `TABLE_SCHEMA` = \'dynamoserver\' AND `TABLE_NAME` = \'user_authorizations\' AND `COLUMN_NAME` = \'target\'';
        result = self._mysql.query(sql)[0]
        # eval the results as a python tuple
        return list(eval(result))

    def check_user_auth(self, user, role, target): #override
        sql = 'SELECT `target` FROM `user_authorizations` WHERE `user_id` = (SELECT `id` FROM `users` WHERE `name` = %s) AND'

        args = (user,)

        if role is None:
            sql += ' `role_id` = 0'
        else:
            sql += ' `role_id` = (SELECT `id` FROM `roles` WHERE `name` = %s)'
            args += (role,)

        targets = self._mysql.query(sql, *args)

        return target in targets

    def list_user_auth(self, user): #override
        sql = 'SELECT r.`name`, a.`target` FROM `user_authorizations` AS a'
        sql += ' LEFT JOIN `roles` AS r ON r.`id` = a.`role_id`'
        sql += ' WHERE a.`user_id` = (SELECT `id` FROM `users` WHERE `name` = %s)'

        return self._mysql.query(sql, user)

    def list_authorized_users(self, target): #override
        sql = 'SELECT u.`name`, s.`name` FROM `user_authorizations` AS a'
        sql += ' INNER JOIN `users` AS u ON u.`id` = a.`user_id`'
        sql += ' INNER JOIN `roles` AS s ON s.`id` = a.`role_id`'

        if target is None:
            sql += ' WHERE a.`target` IS NULL'
            args = tuple()            
        else:
            sql += ' WHERE a.`target` = %s'
            args = (target,)
        
        return self._mysql.query(sql, *args)


class MySQLAppManager(AppManager):
    def __init__(self, config):
        AppManager.__init__(self, config)

        if not hasattr(self, '_mysql'):
            db_params = Configuration(config.db_params)
            db_params.reuse_connection = True # we use locks

            self._mysql = MySQL(db_params)

    def get_applications(self, older_than = 0, status = None, app_id = None, path = None): #override
        sql = 'SELECT `applications`.`id`, `applications`.`write_request`, `applications`.`title`, `applications`.`path`,'
        sql += ' `applications`.`args`, 0+`applications`.`status`, `applications`.`server`, `applications`.`exit_code`, `users`.`name`, `applications`.`user_host`'
        sql += ' FROM `applications` INNER JOIN `users` ON `users`.`id` = `applications`.`user_id`'

        constraints = []
        args = []
        if older_than > 0:
            constraints.append('UNIX_TIMESTAMP(`applications`.`timestamp`) < %s')
            args.append(older_than)
        if status is not None:
            constraints.append('`applications`.`status` = %s')
            args.append(status)
        if app_id is not None:
            constraints.append('`applications`.`id` = %s')
            args.append(app_id)
        if path is not None:
            constraints.append('`applications`.`path` = %s')
            args.append(path)

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        args = tuple(args)

        applications = []
        for aid, write, title, path, args, status, server, exit_code, uname, uhost in self._mysql.xquery(sql, *args):
            applications.append({
                'appid': aid, 'write_request': (write == 1), 'user_name': uname,
                'user_host': uhost, 'title': title, 'path': path, 'args': args,
                'status': int(status), 'server': server, 'exit_code': exit_code
            })

        return applications

    def get_writing_process_id(self): #override
        result = self._mysql.query('SELECT `id` FROM `applications` WHERE `write_request` = 1 AND `status` IN (\'assigned\', \'run\')')
        if len(result) == 0:
            return None
        else:
            return result[0]

    def schedule_application(self, title, path, args, user, host, write_request): #override
        result = self._mysql.query('SELECT `id` FROM `users` WHERE `name` = %s', user)
        if len(result) == 0:
            return 0
        else:
            user_id = result[0]

        sql = 'INSERT INTO `applications` (`write_request`, `title`, `path`, `args`, `user_id`, `user_host`) VALUES (%s, %s, %s, %s, %s, %s)'
        self._mysql.query(sql, write_request, title, path, args, user_id, host)

        return self._mysql.last_insert_id

    def get_next_application(self, read_only): #override
        sql = 'SELECT `applications`.`id`, `write_request`, `title`, `path`, `args`, `users`.`name`, `user_host` FROM `applications`'
        sql += ' INNER JOIN `users` ON `users`.`id` = `applications`.`user_id`'
        sql += ' WHERE `status` = \'new\''
        if read_only:
            sql += ' AND `write_request` = 0'
        sql += ' ORDER BY `applications`.`id` LIMIT 1'
        result = self._mysql.query(sql)

        if len(result) == 0:
            return None
        else:
            appid, write_request, title, path, args, uname, uhost = result[0]
            return {
                'appid': appid, 'write_request': (write_request == 1), 'user_name': uname,
                'user_host': uhost, 'title': title, 'path': path, 'args': args
            }

    def update_application(self, app_id, **kwd): #override
        sql = 'UPDATE `applications` SET '

        args = []
        updates = []

        if 'status' in kwd:
            updates.append('`status` = %s')
            args.append(ServerManager.application_status_name(kwd['status']))

        if 'hostname' in kwd:
            updates.append('`server` = %s')
            args.append(kwd['hostname'])

        if 'exit_code' in kwd:
            updates.append('`exit_code` = %s')
            args.append(kwd['exit_code'])

        if 'path' in kwd:
            updates.append('`path` = %s')
            args.append(kwd['path'])

        sql += ', '.join(updates)

        sql += ' WHERE `id` = %s'
        args.append(app_id)

        self._mysql.query(sql, *tuple(args))

    def delete_application(self, app_id): #override
        self._mysql.query('DELETE FROM `applications` WHERE `id` = %s', app_id)

    def check_application_auth(self, title, user, checksum): #override
        result = self._mysql.query('SELECT `id` FROM `users` WHERE `name` = %s', user)
        if len(result) == 0:
            return False

        user_id = result[0]

        sql = 'SELECT `user_id` FROM `authorized_applications` WHERE `title` = %s AND `checksum` = UNHEX(%s)'
        for auth_user_id in self._mysql.query(sql, title, checksum):
            if auth_user_id == 0 or auth_user_id == user_id:
                return True

        return False

    def list_authorized_applications(self, titles = None, users = None, checksums = None): #override
        sql = 'SELECT a.`title`, u.`name`, HEX(a.`checksum`) FROM `authorized_applications` AS a'
        sql += ' LEFT JOIN `users` AS u ON u.`id` = a.`user_id`'

        constraints = []
        args = []
        if type(titles) is list:
            constraints.append('a.`title` IN (%s)' % ','.join(['%s'] * len(titles)))
            args.extend(titles)

        if type(users) is list:
            constraints.append('u.`name` IN (%s)' % ','.join(['%s'] * len(users)))
            args.extend(users)

        if type(checksums) is list:
            constraints.append('a.`checksum` IN (%s)' % ','.join(['UNHEX(%s)'] * len(checksums)))
            args.extend(checksums)

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        return self._mysql.query(sql, *tuple(args))

    def authorize_application(self, title, checksum, user = None): #override
        sql = 'INSERT INTO `authorized_applications` (`user_id`, `title`, `checksum`)'
        if user is None:
            sql += ' VALUES (0, %s, UNHEX(%s))'
            args = (title, checksum)
        else:
            sql += ' SELECT u.`id`, %s, UNHEX(%s) FROM `users` AS u WHERE u.`name` = %s'
            args = (title, checksum, user)

        inserted = self._mysql.query(sql, *args)
        return inserted != 0

    def revoke_application_authorization(self, title, user = None): #override
        sql = 'DELETE FROM `authorized_applications` WHERE (`user_id`, `title`) ='
        if user is None:
            sql += ' (0, %s)'
            args = (title,)
        else:
            sql += ' (SELECT u.`id`, %s FROM `users` AS u WHERE u.`name` = %s)'
            args = (title, user)

        deleted = self._mysql.query(sql, *args)
        return deleted != 0

    def register_sequence(self, name, user, restart = False): #override
        sql = 'INSERT INTO `application_sequences` (`name`, `user_id`, `restart`) SELECT %s, `id`, %s FROM `users` WHERE `name` = %s'
        inserted = self._mysql.query(sql, name, 1 if restart else 0, user)
        return inserted != 0

    def find_sequence(self, name): #override
        sql = 'SELECT u.`name`, s.`restart`, s.`status` FROM `application_sequences` AS s'
        sql += ' INNER JOIN `users` AS u ON u.`id` = s.`user_id`'
        sql += ' WHERE s.`name` = %s'

        try:
            user, restart, status = self._mysql.query(sql, name)[0]
        except IndexError:
            return None

        return (name, user, (restart != 0), status == 'enabled')

    def update_sequence(self, name, restart = None, enabled = None): #override
        if restart is None and enabled is None:
            return True

        changes = []
        args = []

        if restart is not None:
            changes.append('`restart` = %s')
            args.append(1 if restart else 0)
        if enabled is not None:
            changes.append('`status` = %s')
            args.append('enabled' if enabled else 'disabled')

        args.append(name)

        sql = 'UPDATE `application_sequences` SET ' + ', '.join(changes) + ' WHERE `name` = %s'

        updated = self._mysql.query(sql, *tuple(args))
        return updated != 0

    def delete_sequence(self, name): #override
        sql = 'DELETE FROM `application_sequences` WHERE `name` = %s'
        deleted = self._mysql.query(sql, name)
        return deleted != 0

    def get_sequences(self, enabled_only = True): #override
        sql = 'SELECT `name` FROM `application_sequences`'
        if enabled_only:
            sql += ' WHERE `status` = \'enabled\''

        return self._mysql.query(sql)


class MySQLMasterServer(MySQLAuthorizer, MySQLAppManager, MasterServer):
    def __init__(self, config):
        MySQLAuthorizer.__init__(self, config)
        MySQLAppManager.__init__(self, config)
        MasterServer.__init__(self, config)

        self._server_id = 0

    def _connect(self): #override
        self._mysql.lock_tables(write = ['servers'])

        if self.get_master_host() == 'localhost' or self.get_master_host() == socket.gethostname():
            # This is the master server; wipe the table clean
            self._mysql.query('DELETE FROM `servers`')
            self._mysql.query('ALTER TABLE `servers` AUTO_INCREMENT = 1')
        else:
            self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', socket.gethostname())

        self._mysql.query('INSERT INTO `servers` (`hostname`, `last_heartbeat`) VALUES (%s, NOW())', socket.gethostname())
        # id of this server
        self._server_id = self._mysql.last_insert_id

        self._mysql.unlock_tables()

    def lock(self): #override
        self._mysql.lock_tables(write = ['servers', 'applications'], read = ['users'])

    def unlock(self): #override
        self._mysql.unlock_tables()

    def get_master_host(self): #override
        return self._mysql.hostname()

    def set_status(self, status, hostname): #override
        self._mysql.query('UPDATE `servers` SET `status` = %s WHERE `hostname` = %s', ServerManager.server_status_name(status), hostname)

    def get_status(self, hostname): #override
        result = self._mysql.query('SELECT `status` FROM `servers` WHERE `hostname` = %s', hostname)
        if len(result) == 0:
            return None
        else:
            return ServerManager.server_status_val(result[0])

    def get_host_list(self, status = None, detail = False): #override
        if detail:
            sql = 'SELECT `hostname`, `last_heartbeat`, `status`, `store_host`, `store_module`,'
            sql += ' `shadow_module`, `shadow_config`, `board_module`, `board_config`'
        else:
            sql = 'SELECT `hostname`, `status`, `store_module` IS NOT NULL'

        sql += ' FROM `servers`'
        if status != None:
            sql += ' WHERE `status` = \'%s\'' % ServerManager.server_status_name(status)

        sql += ' ORDER BY `id`'

        return self._mysql.query(sql)

    def copy(self, remote_master):
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
        self._mysql.insert_many('roles', fields, None, all_roles, do_update = True)

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
        while self.get_status(hostname) == ServerManager.SRV_UPDATING:
            # need to get the version of the remote server when it's not updating
            self._mysql.unlock_tables()
            time.sleep(2)
            self._mysql.lock_tables(read = ['servers'])
        
        sql = 'SELECT `store_module`, `store_config`, `store_version` FROM `servers` WHERE `hostname` = %s'
        result = self._mysql.query(sql, hostname)
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

    def create_authorizer(self): #override
        config = Configuration(db_params = self._mysql.config())
        return MySQLAuthorizer(config)

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
