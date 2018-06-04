import socket

from dynamo.core.components.appmanager import AppManager
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class MySQLAppManager(AppManager):
    def __init__(self, config):
        AppManager.__init__(self, config)

        if not hasattr(self, '_mysql'):
            db_params = Configuration(config.db_params)
            db_params.reuse_connection = True # we use locks

            self._mysql = MySQL(db_params)

        # make sure applications row with id 0 exists
        count = self._mysql.query('SELECT COUNT(*) FROM `applications` WHERE `id` = 0')[0]
        if count == 0:
            # Cannot insert with id = 0 (will be interpreted as next auto_increment id unless server-wide setting is changed)
            # Inesrt with an implicit id first and update later
            sql = 'INSERT INTO `applications` (`write_request`, `title`, `path`, `status`, `user_id`, `user_host`)'
            sql += ' VALUES (1, \'wsgi\', \'\', \'done\', 0, \'\')'
            self._mysql.query(sql)
            self._mysql.query('UPDATE `applications` SET `id` = 0 WHERE `id` = %s', self._mysql.last_insert_id)

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

    def get_writing_process_host(self): #override
        result = self._mysql.query('SELECT `server` FROM `applications` WHERE `write_request` = 1 AND `status` IN (\'assigned\', \'run\')')
        if len(result) == 0:
            return None
        else:
            return result[0]

    def get_web_write_process_id(self): #override
        return self._mysql.query('SELECT `user_id` FROM `applications` WHERE `id` = 0')[0]

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
            args.append(AppManager.status_name(kwd['status']))

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

    def start_write_web(self, host, pid): #override
        # repurposing user_id for pid
        sql = 'UPDATE `applications` SET `status` = \'run\', `server` = %s, `user_host` = %s, `user_id` = %s WHERE `id` = 0'
        self._mysql.query(sql, host, host, pid)

    def stop_write_web(self): #override
        # We don't actually use the host name because there is only one slot for web write anyway
        sql = 'UPDATE `applications` SET `status` = \'done\', `server` = \'\', `user_host` = \'\', `user_id` = 0 WHERE `id` = 0'
        self._mysql.query(sql)

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

