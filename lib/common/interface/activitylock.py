import time
import logging

from common.interface.mysql import MySQL
import common.configuration as config

logger = logging.getLogger(__name__)

class ActivityLock(object):
    """
    Web-based activity lock using registry.
    """

    def __init__(self, application, service = 'dynamo', asuser = '', db_params = config.activitylock.db_params):
        self._mysql = MySQL(**db_params)

        self.application = application
        self.service = service
        if asuser:
            self.user = asuser
        else:
            self.user = config.activitylock.default_user

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
        if len(result) != 0:
            logger.error('Lock logic error: some process obtained the activity lock for %s', self.application)
            self._mysql.query('UNLOCK TABLES')
            return True

        query = 'DELETE FROM `activity_lock` WHERE `application` = %s'
        self._mysql.query(query, self.application)

        self._mysql.query('UNLOCK TABLES')

        # return True for "with lock" use case
        return True
