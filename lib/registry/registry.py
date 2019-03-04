from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class RegistryDatabase(object):
    """
    Similar to HistoryDatabase, this is just one abstraction layer that doesn't really hide the
    backend technology for the registry. We still have the benefit of being able to use default
    parameters to initialize the registry database handle.
    """

        # default configuration
    _config = Configuration()

    @staticmethod
    def set_default(config):
        RegistryDatabase._config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = RegistryDatabase._config

        self.db = MySQL(config.db_params)

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = True

    def get_locked_apps(self):
        sql = 'SELECT DISTINCT `application` FROM `activity_lock`'
        return self.db.query(sql)

    def get_app_lock(self, app):
        # this function can be called within a table lock, so we need to lock what we use
        self.db.lock_tables(read = [('activity_lock', 'l'), ('user_services', 's')])

        sql = 'SELECT l.`user`, s.`name`, UNIX_TIMESTAMP(l.`timestamp`), l.`note` FROM `activity_lock` AS l'
        sql += ' LEFT JOIN `user_services` AS s ON s.`id` = l.`service_id`'
        sql += ' WHERE l.`application` = %s ORDER BY l.`timestamp` ASC';

        lock_data = self.db.query(sql, app)

        self.db.unlock_tables()

        if len(lock_data) == 0:
            return None, None, None, None, 0

        first_user, first_service, lock_time, note = lock_data[0]

        depth = 1
        
        for user, service, _, _ in lock_data[1:]:
            if user == first_user and service == first_service:
                depth += 1
                
        return first_user, first_service, lock_time, note, depth

    def lock_app(self, app, user, service = None, note = None):
        if service is None:
            service_id = 0
        else:
            try:
                sql = 'SELECT `id` FROM `user_services` WHERE `name` = %s'
                service_id = self.db.query(sql, service)[0]
            except IndexError:
                service_id = 0

        sql = 'INSERT INTO `activity_lock` (`user`, `service_id`, `application`, `timestamp`, `note`)'
        sql += ' VALUES (%s, %s, %s, NOW(), %s)'

        self.db.query(sql, user, service_id, app, note)

    def unlock_app(self, app, user, service = None):
        if service is None:
            service_id = 0
        else:

            try:
                sql = 'SELECT `id` FROM `user_services` WHERE `name` = %s'
                service_id = self.db.query(sql, service)[0]
            except IndexError:
                service_id = 0

        self.db.lock_tables(write = ['activity_lock', ('activity_lock', 'l')])

        sql = 'DELETE FROM `activity_lock` WHERE `id` = ('
        sql += ' SELECT m FROM ('
        sql += '  SELECT MAX(`id`) m FROM `activity_lock` AS l'
        sql += '  WHERE `user` = %s AND `service_id` = %s AND `application` = %s'
        sql += ' ) AS tmp'
        sql += ')'
        self.db.query(sql, user, service_id, app)

        # a little cleanup
        if self.db.query('SELECT COUNT(*) FROM `activity_lock`')[0] == 0:
            self.db.query('ALTER TABLE `activity_lock` AUTO_INCREMENT = 1')

        self.db.unlock_tables()



class CacheDatabase(RegistryDatabase):
    """
    Similar to HistoryDatabase, this is just one abstraction layer that doesn't really hide the
    backend technology for the registry. We still have the benefit of being able to use default
    parameters to initialize the registry database handle.
    """

        # default configuration
    _config = Configuration()

    @staticmethod
    def set_default(config):
        CacheDatabase._config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = CacheDatabase._config

        self.db = MySQL(config.db_params)

        self.set_read_only(config.get('read_only', False))

