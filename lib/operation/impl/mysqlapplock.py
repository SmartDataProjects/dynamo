import logging

from dynamo.operation.applock import ApplicationLockInterface
from dynamo.utils.interface import MySQL

LOG = logging.getLogger(__name__)

class MySQLApplicationLockInterface(ApplicationLockInterface):

    def __init__(self, config):
        ApplicationLockInterface.__init__(self, config)

        self._registry = MySQL(config.db_config)

    def check(self): # override
        query = 'SELECT `users`.`name`, `services`.`name` FROM `activity_lock`'
        query += ' INNER JOIN `users` ON `users`.`id` = `activity_lock`.`user_id`'
        query += ' INNER JOIN `services` ON `services`.`id` = `activity_lock`.`service_id`'
        query += ' WHERE `application` = %s ORDER BY `timestamp` ASC LIMIT 1'
        result = self._registry.query(query, self.app)
        if len(result) == 0:
            return None

        return result[0]

    def lock(self): # override
        query = 'INSERT INTO `activity_lock` (`user_id`, `service_id`, `application`, `timestamp`, `note`)'
        query += ' SELECT `users`.`id`, `services`.`id`, %s, NOW(), \'Dynamo running\' FROM `users`, `services`'
        query += ' WHERE `users`.`name` = %s AND `services`.`name` = %s'
        self._registry.query(query, self.app, self.user, self.service)

        while True:
            owner = self.check()
            if owner is None:
                raise RuntimeError('Lock logic error - lock disappeared')

            elif owner == (self.user, self.service):
                break

            LOG.info('Activity lock for %s in place: user = %s, service = %s', self.app, *owner)
            time.sleep(30)

        LOG.info('Locked system for %s', self.app)

    def unlock(self): # override
        query = 'DELETE FROM l USING `activity_lock` AS l'
        query += ' INNER JOIN `users` AS u ON u.`id` = l.`user_id`'
        query += ' INNER JOIN `services` AS s ON s.`id` = l.`service_id`'
        query += ' WHERE u.`name` = %s AND s.`name` = %s AND l.`application` = %s AND l.`timestamp` = ('
        query += 'SELECT x.t FROM ('
        query += 'SELECT MAX(`timestamp`) AS t FROM `activity_lock` WHERE'
        query += ' `user_id` = l.`user_id` AND `service_id` = l.`service_id` AND `application` = l.`application`'
        query += ') AS x'
        query += ') LIMIT 1'

        self._registry.query(query, self.user, self.service, self.app)
