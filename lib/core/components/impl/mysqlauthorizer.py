import logging

from dynamo.core.components.authorizer import Authorizer
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)

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

    def identify_user(self, dn = '', check_trunc = False, name = '', uid = None): #override
        if dn:
            result = self._mysql.query('SELECT `name`, `id`, `dn` FROM `users` WHERE `dn` = %s', dn)
            if check_trunc and len(result) == 0:
                while dn:
                    dn = dn[:dn.rfind('/')]
                    result = self._mysql.query('SELECT `name`, `id`, `dn` FROM `users` WHERE `dn` = %s', dn)
                    if len(result) != 0:
                        break
        elif name:
            result = self._mysql.query('SELECT `name`, `id`, `dn` FROM `users` WHERE `name` = %s', name)
        else:
            result = self._mysql.query('SELECT `name`, `id`, `dn` FROM `users` WHERE `id` = %s', uid)

        if len(result) == 0:
            return None
        else:
            return (result[0][0], int(result[0][1]), result[0][2])

    def identify_role(self, name): #override
        try:
            name, rid = self._mysql.query('SELECT `name`, `id` FROM `roles` WHERE `name` = %s', name)[0]
        except IndexError:
            return None
        else:
            return (name, int(rid))

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

        if target is None:
            return len(targets) != 0
        else:
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

        if target is not None:
            sql += ' WHERE a.`target` = %s'
            args = (target,)
        
        return self._mysql.query(sql, *args)

    def create_authorizer(self): #override
        if self.readonly_config is None:
            db_params = self._mysql.config()
        else:
            db_params = self.readonly_config.db_params

        config = Configuration(db_params = db_params)
        return MySQLAuthorizer(config)
