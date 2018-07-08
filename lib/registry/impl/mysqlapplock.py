from dynamo.registry.applock import Applock
from dynamo.utils.interface.mysql import MySQL

class MySQLApplock(Applock):
    def __init__(self, config):
        self._mysql = MySQL(config.db_params)

    def get_locked_apps(self):
        sql = 'SELECT DISTINCT `application` FROM `activity_lock`'
        return self._mysql.query(sql)
