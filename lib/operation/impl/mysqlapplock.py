import logging
import time

from dynamo.operation.applock import ApplicationLockInterface
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class MySQLApplicationLockInterface(ApplicationLockInterface):

    def __init__(self, config, authorizer):
        ApplicationLockInterface.__init__(self, config, authorizer)

        self.user_id = authorizer.identify_user(name = self.user, with_id = True)[1]
        self.role_id = authorizer.identify_role(name = self.role, with_id = True)[1]

        self._registry = MySQL(config.get('db_params', None))

    def lock(self): # override
        query = 'INSERT INTO `activity_lock` (`user_id`, `role_id`, `application`, `timestamp`, `note`)'
        query += ' VALUES (%s, %s, %s, NOW(), \'Dynamo running\')'
        self._registry.query(query, self.user_id, self.role_id, self.app)

        while True:
            query = 'SELECT `user_id`, `role_id` FROM `activity_lock` WHERE `application` = %s ORDER BY `timestamp` ASC LIMIT 1'
            result = self._registry.query(query, self.app)
            if len(result) == 0:
                raise RuntimeError('Lock logic error - lock disappeared')

            if result[0] == (self.user_id, self.role_id):
                break

            LOG.info('Activity lock for %s in place: user = %s, role = %s', self.app, *result[0])
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

        self._registry.query(query, self.user_id, self.role_id, self.app, self.user_id, self.role_id, self.app)
