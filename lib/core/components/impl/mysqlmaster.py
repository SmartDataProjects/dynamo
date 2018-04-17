import json
import socket

from dynamo.core.components.master import MasterServer
from dynamo.core.manager import ServerManager
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class MySQLMasterServer(MasterServer):
    def __init__(self, config):
        MasterServer.__init__(self, config)

        db_params = Configuration(config.db_params)
        db_params.reuse_connection = True # we use locks

        self._mysql = MySQL(db_params)
        self._server_id = 0

    def _connect(self): #override
        self._mysql.query('LOCK TABLES `servers` WRITE')

        if self.get_master_host() == 'localhost' or self.get_master_host() == socket.gethostname():
            # This is the master server; wipe the table clean
            self._mysql.query('DELETE FROM `servers`')
            self._mysql.query('ALTER TABLE `servers` AUTO_INCREMENT = 1')
        else:
            self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', socket.gethostname())

        # id of this server
        self._server_id = self._mysql.query('INSERT INTO `servers` (`hostname`, `last_heartbeat`) VALUES (%s, NOW())', socket.gethostname())

        self._mysql.query('UNLOCK TABLES')

    def lock(self): #override
        self._mysql.query('LOCK TABLES `servers` WRITE, `applications` WRITE, `users` READ')

    def unlock(self): #override
        self._mysql.query('UNLOCK TABLES')

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

    def get_user_list(self): #override
        return self._mysql.query('SELECT `name`, `email`, `dn` FROM `users` ORDER BY `id`')

    def copy(self, remote_master):
        self._mysql.query('DELETE FROM `servers`')
        self._mysql.query('ALTER TABLE `servers` AUTO_INCREMENT = 1')

        all_servers = remote_master.get_host_list(detail = True)
        fields = ('hostname', 'last_heartbeat', 'status', 'store_host', 'store_module', 'shadow_module', 'shadow_config', 'board_module', 'board_config')
        self._mysql.insert_many('servers', fields, None, all_servers, do_update = True)

        self._mysql.query('DELETE FROM `users`')
        self._mysql.query('ALTER TABLE `users` AUTO_INCREMENT = 1')

        all_users = remote_master.get_user_list()
        fields = ('name', 'email', 'dn')
        self._mysql.insert_many('users', fields, None, all_users, do_update = True)

    def get_next_master(self, current): #override
        self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', current)
        
        # shadow config must be the same as master
        result = self._mysql.query('SELECT `shadow_module`, `shadow_config` FROM `servers` ORDER BY `id` LIMIT 1')
        if len(result) == 0:
            raise RuntimeError('No servers can become master at this moment')

        module, config_str = result[0]
        return module, Configuration(json.loads(config_str))

    def get_applications(self, older_than = 0, app_id = None): #override
        sql = 'SELECT `applications`.`id`, `applications`.`write_request`, `applications`.`title`, `applications`.`path`,'
        sql += ' `applications`.`args`, 0+`applications`.`status`, `applications`.`server`, `applications`.`exit_code`, `users`.`name`'
        sql += ' FROM `applications` INNER JOIN `users` ON `users`.`id` = `applications`.`user_id`'

        constraints = []
        if older_than > 0:
            constraints.append('UNIX_TIMESTAMP(`applications`.`timestamp`) < UNIX_TIMESTAMP() - %d' % older_than)
        if app_id is not None:
            constraints.append('`applications`.`id` = %d' % app_id)

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        applications = []
        for aid, write, title, path, args, status, server, exit_code, uname in self._mysql.xquery(sql):
            applications.append({'appid': aid, 'write_request': (write == 1), 'user_name': uname, 'title': title,
                'path': path, 'args': args, 'status': int(status), 'server': server, 'exit_code': exit_code})

        return applications

    def get_writing_process_id(self): #override
        result = self._mysql.query('SELECT `id` FROM `applications` WHERE `write_request` = 1 AND `status` = \'run\'')
        if len(result) == 0:
            return None
        else:
            return result[0]

    def schedule_application(self, title, path, args, user, write_request): #override
        result = self._mysql.query('SELECT `id` FROM `users` WHERE `name` = %s', user)
        if len(result) == 0:
            return 0
        else:
            user_id = result[0]

        sql = 'INSERT INTO `applications` (`write_request`, `title`, `path`, `args`, `user_id`) VALUES (%s, %s, %s, %s, %s)'
        return self._mysql.query(sql, write_request, title, path, args, user_id)

    def get_next_application(self, read_only): #override
        sql = 'SELECT `applications`.`id`, `write_request`, `title`, `path`, `args`, `users`.`name` FROM `applications`'
        sql += ' INNER JOIN `users` ON `users`.`id` = `applications`.`user_id`'
        sql += ' WHERE `status` = \'new\''
        if read_only:
            sql += ' AND `write_request` = 0'
        sql += ' ORDER BY `timestamp` LIMIT 1'
        result = self._mysql.query(sql)

        if len(result) == 0:
            return None
        else:
            return result[0]

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

    def advertise_store(self, module, config): #override
        config = config.clone()
        if config.db_params.host == 'localhost':
            config.db_params.host = socket.gethostname()

        sql = 'UPDATE `servers` SET `store_module` = %s, `store_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self._server_id)

    def get_store_config(self, hostname): #override
        sql = 'SELECT `store_module`, `store_config` FROM `servers` WHERE `hostname` = %s'
        result = self._mysql.query(sql, hostname)
        if len(result) == 0:
            return None

        module, config_str = result[0]

        return module, Configuration(json.loads(config_str))

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

    def identify_user(self, dn): #override
        result = self._mysql.query('SELECT `name` FROM `users` WHERE `dn` = %s', dn)
        if len(result) == 0:
            return None
        else:
            return result[0]

    def authorize_user(self, user, service): #override
        sql = 'SELECT COUNT(*) FROM `authorized_users` AS a'
        sql += ' INNER JOIN `users` AS u ON u.`id` = a.`user_id`'
        sql += ' INNER JOIN `services` AS s ON s.`id` = a.`service_id`'
        sql += ' WHERE u.`name` = %s AND s.`name` = %s'
        return (self._mysql.query(sql, user, service)[0] != 0)

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
