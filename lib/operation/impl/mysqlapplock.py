import logging
import time

from dynamo.operation.applock import ApplicationLockInterface
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class MySQLApplicationLockInterface(ApplicationLockInterface):

    def __init__(self, config, authorizer):
        ApplicationLockInterface.__init__(self, config, authorizer)

        self._registry = MySQL(config.get('db_params', None))

    def check(self): # override
        query = 'SELECT `user`, `role` FROM `activity_lock` WHERE `application` = %s ORDER BY `timestamp` ASC LIMIT 1'
        result = self._registry.query(query, self.app)
        if len(result) == 0:
            return None

        return result[0]

    def lock(self): # override
        query = 'INSERT INTO `activity_lock` (`user`, `role`, `application`, `timestamp`, `note`)'
        query += ' VALUES (%s, %s, %s, NOW(), \'Dynamo running\')'
        self._registry.query(query, self.user, self.role, self.app)

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
        query += ' WHERE `user` = %s AND `role` = %s AND `application` = %s AND `timestamp` = ('
        query += 'SELECT x.t FROM ('
        query += 'SELECT MAX(`timestamp`) AS t FROM `activity_lock` WHERE'
        query += ' `user` = %s AND `role` = %s AND `application` = %s'
        query += ') AS x'
        query += ') LIMIT 1'

        self._registry.query(query, self.user, self.role, self.app, self.user, self.role, self.app)
