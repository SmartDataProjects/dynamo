import time
import logging

from common.interface.mysql import MySQL
from common.interface.classes import default_interface
import common.configuration as config

logger = logging.getLogger(__name__)

class ActivityLock(object):
    """
    Web-based activity lock using registry.
    """

    def __init__(self, application, service = 'dynamo', asuser = '', db_params = config.registry.db_params):
        self._mysql = MySQL(**db_params)

        self.application = application
        self.service = service
        if asuser:
            self.user = asuser
        else:
            self.user = config.activitylock.default_user

        self._userinfo = default_interface['user_source']()

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.unlock():
            raise RuntimeError('Failed to unlock')

        return exc_type is None and exc_value is None and traceback is None

    def lock(self):
        while True:
            self._mysql.query('LOCK TABLES `activity_lock` WRITE, `users` WRITE, `services` WRITE')

            query = 'SELECT `users`.`name`, `services`.`name` FROM `activity_lock`'
            query += ' INNER JOIN `users` ON `users`.`id` = `activity_lock`.`user_id`'
            query += ' INNER JOIN `services` ON `services`.`id` = `activity_lock`.`service_id`'
            query += ' WHERE `application` = %s'
            result = self._mysql.query(query, self.application)
            if len(result) == 0:
                break

            elif result[0] == (self.user, self.service):
                query = 'DELETE FROM `activity_lock` WHERE `application` = %s'
                self._mysql.query(query, self.application)
                break

            logger.info('Activity lock for %s in place: user = %s, service = %s', self.application, *result[0])
            self._mysql.query('UNLOCK TABLES')
            time.sleep(60)

        if self._mysql.query('SELECT COUNT(*) FROM `users` WHERE `name` = %s', self.user)[0] == 0:
            user_data = self._userinfo.get_user(self.user)
            if user_data is None:
                raise RuntimeError('Invalid user %s used for activity lock' % self.user)

            self._mysql.query('INSERT INTO `users` (`name`, `email`, `dn`) VALUES (%s, %s, %s)', *user_data)

        query = 'INSERT INTO `activity_lock` (`user_id`, `service_id`, `application`, `timestamp`, `note`)'
        query += ' SELECT `users`.`id`, `services`.`id`, %s, NOW(), \'Dynamo running\' FROM `users`, `services`'
        query += ' WHERE `users`.`name` = %s AND `services`.`name` = %s'
        self._mysql.query(query, self.application, self.user, self.service)

        self._mysql.query('UNLOCK TABLES')

        logger.info('Locked system for %s', self.application)

    def unlock(self):
        self._mysql.query('LOCK TABLES `activity_lock` WRITE, `users` WRITE, `services` WRITE')

        query = 'SELECT `users`.`name`, `services`.`name` FROM `activity_lock`'
        query += ' INNER JOIN `users` ON `users`.`id` = `activity_lock`.`user_id`'
        query += ' INNER JOIN `services` ON `services`.`id` = `activity_lock`.`service_id`'
        query += ' WHERE `application` = %s'
        result = self._mysql.query(query, self.application)
        if len(result) == 0:
            self._mysql.query('UNLOCK TABLES')
            return True

        if result[0] == (self.user, self.service):
            query = 'DELETE FROM `activity_lock` WHERE `application` = %s'
            self._mysql.query(query, self.application)
            self._mysql.query('UNLOCK TABLES')
            return True

        else:
            logger.error('Lock logic error: some process obtained the activity lock for %s', self.application)
            self._mysql.query('UNLOCK TABLES')
            return False
