import socket

from dynamo.core.master.base import MasterServer
from dynamo.core.manager import ServerManager
from dynamo.utils.interface import MySQL
from dynamo.dataformat import Configuration

class MySQLMasterServer(MasterServer):
    def __init__(self, config):
        MasterServer.__init__(self, config)

        db_params = Configuration(config.db_params)
        db_params.reuse_connection = True # we use locks

        self._mysql = MySQL(db_params)

        self.server_id = self._mysql.query('INSERT INTO `servers` (`hostname`, `last_heartbeat`) VALUES (%s, NOW)', socket.gethostname())

    def lock(self): #override
        self._mysql.query('LOCK TABLES `servers` WRITE, `applications` WRITE')

    def unlock(self): #override
        self._mysql.query('UNLOCK TABLES')

    def set_status(self, status, hostname): #override
        self._mysql.query('UPDATE `servers` SET `status` = %s WHERE `hostname` = %s', ServerManager.server_status_name(status), hostname)

    def get_status(self, hostname): #override
        result = self._mysql.query('SELECT `status` FROM `servers` WHERE `hostname` = %s', hostname)
        if len(result) == 0:
            return None
        else:
            return ServerManager.server_status_val(result[0])

    def get_host_list(self, status = None): #override
        sql = 'SELECT `hostname`, `status`, `store_module` IS NOT NULL FROM `servers`'
        args = (,)
        if status != None:
            sql += ' WHERE `status` = %s'
            args += (ServerManager.server_status_name(status))

        return self._mysql.query(sql, *args)

    def get_writing_process_id(self): #override
        result = self_mysql.query('SELECT `id` FROM `applications` WHERE `write_request` = 1 AND `status` = \'run\'')
        if len(result) == 0:
            return None
        else:
            return result[0]

    def schedule_application(self, title, path, args, user, write_request): #override
        sql = 'INSERT INTO `applications` (`write_request`, `title`, `path`, `args`, `user`) VALUES (%s, %s, %s, %s, %s)'
        return self._mysql.query(sql, write_request, title, path, args, user)

    def get_next_application(self, read_only): #override
        sql = 'SELECT `id`, `write_request`, `title`, `path`, `args`, `user` FROM `applications`'
        sql += ' WHERE `status` = \'new\''
        if read_only:
            sql += ' AND `write_request` = 0'
        sql += ' ORDER BY `timestamp` LIMIT 1'
        result = self._mysql.query(sql)

        if len(result) == 0:
            return None
        else:
            return result[0]

    def set_application_status(self, status, app_id, hostname = None, exit_code = None): #override
        args = (ServerManager.application_status_name(status),)

        sql = 'UPDATE `applications` SET `status` = %s'

        if hostname is not None:
            sql += ', `server` = %s'
            args += (hostname,)

        if exit_code is not None:
            sql += ', `exit_code` = %s'
            args += (exit_code,)

        sql += ' WHERE `id` = %s'
        args += (app_id,)

        self._mysql.query(sql, *args)

    def get_application_status(self, app_id): #override
        result = self._mysql.query('SELECT `status` FROM `applications` WHERE `id` = %s', app_id)
        if len(result) == 0:
            # don't know what happened but the application is gone
            return None
        else:
            return ServerManager.application_status_val(result[0])

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
        sql = 'UPDATE `servers` SET `store_module` = %s, `store_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self.server_id)

    def get_store_config(self, hostname): #override
        sql = 'SELECT `store_module`, `store_config` FROM `servers` WHERE `hostname` = %s'
        result = self._mysql.query(sql, hostname)
        if len(result) == 0:
            return None

        module, config_str = result[0]

        return module, Configuration(json.loads(config_str))

    def advertise_board(self, module, config): #override
        sql = 'UPDATE `servers` SET `board_module` = %s, `board_config` = %s WHERE `id` = %s'
        self._mysql.query(sql, module, config.dump_json(), self.server_id)

    def get_board_config(self, hostname): #override
        sql = 'SELECT `board_module`, `board_config` FROM `servers` WHERE `hostname` = %s'
        result = self._mysql.query(sql, hostname)
        if len(result) == 0:
            return None

        module, config_str = result[0]

        return module, Configuration(json.loads(config_str))

    def declare_remote_store(self, hostname): #override
        server_id = self._mysql.query('SELECT `id` FROM `servers` WHERE `hostname` = %s', hostname)
        self._mysql.query('UPDATE `servers` SET `store_host` = %s WHERE `id` = %s', server_id, self.server_id)

    def send_heartbeat(self): #override
        self._mysql.query('UPDATE `servers` SET `last_heartbeat` = NOW() WHERE `id` = %s', self.server_id)

    def disconnect(self): #override
        self._mysql.query('DELETE FROM `servers` WHERE `id` = %s', self.server_id)
        self._mysql.close()
