import logging
import time

from dynamo.operation.applock import ApplicationLockInterface
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class MySQLApplicationLockInterface(ApplicationLockInterface):

    def __init__(self, config):
        ApplicationLockInterface.__init__(self, config)

        self._registry = MySQL(config.get('db_config', None))

        query = 'SELECT `id` FROM `users` WHERE `name` = %s'
        result = self._registry.query(query, self.user)
        if len(result) == 0:
            raise RuntimeError('User %s is not in registry' % self.user)

        self._uid = result[0]

        query = 'SELECT `id` FROM `roles` WHERE `name` = %s'
        result = self._registry.query(query, self.role)
        if len(result) == 0:
            raise RuntimeError('Role %s is not in registry' % self.role)

        self._sid = result[0]

    def check(self): # override
        query = 'SELECT `users`.`name`, `roles`.`name` FROM `activity_lock`'
        query += ' INNER JOIN `users` ON `users`.`id` = `activity_lock`.`user_id`'
        query += ' INNER JOIN `roles` ON `roles`.`id` = `activity_lock`.`role_id`'
        query += ' WHERE `application` = %s ORDER BY `timestamp` ASC LIMIT 1'
        result = self._registry.query(query, self.app)
        if len(result) == 0:
            return None

        return result[0]

    def lock(self): # override
        query = 'INSERT INTO `activity_lock` (`user_id`, `role_id`, `application`, `timestamp`, `note`)'
        query += ' VALUES (%s, %s, %s, NOW(), \'Dynamo running\')'
        self._registry.query(query, self._uid, self._sid, self.app)

        while True:
            owner = self.check()
            if owner is None:
                raise RuntimeError('Lock logic error - lock disappeared')

            elif owner == (self.user, self.role):
                break

            LOG.info('Activity lock for %s in place: user = %s, role = %s', self.app, *owner)
            time.sleep(30)

        LOG.info('Locked system for %s', self.app)

    def unlock(self): # override

        query = 'DELETE FROM `activity_lock`'
        query += ' WHERE `user_id` = %s AND `role_id` = %s AND `application` = %s AND `timestamp` = ('
        query += 'SELECT x.t FROM ('
        query += 'SELECT MAX(`timestamp`) AS t FROM `activity_lock` WHERE'
        query += ' `user_id` = %s AND `role_id` = %s AND `application` = %s'
        query += ') AS x'
        query += ') LIMIT 1'

        self._registry.query(query, self._uid, self._sid, self.app, self._uid, self._sid, self.app)
