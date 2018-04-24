def create_detox_lock(self, item, sites, groups, expiration_date, comment = None):
    sql = 'INSERT INTO `detox_locks` (`item`, `sites`, `groups`, `lock_date`, `expiration_date`, `user_id`, `service_id`, `comment`)'
    sql += ' VALUES (%s, %s, %s, NOW(), FROM_UNIXTIME(%s), %s, %s, %s)';

def get_detox_locks(self, users = None):
    sql = 'SELECT `item`, `sites`, `groups` FROM `detox_locks` WHERE `unlock_date` IS NULL'
    if users is not None and len(users) != 0:
        sql += ' AND (`user_id`, `service_id`) IN ('
        sql += 'SELECT u.`id`, s.`id` FROM `users` AS u, `services` AS s WHERE '
        sql += ' OR '.join('(u.`name` LIKE "%s" AND s.`name` LIKE "%s")' % us for us in self.users)
        sql += ')'

    return self.backend.query(sql)

from dynamo.registry.registry import DynamoRegistry

DynamoRegistry.create_detox_lock = create_detox_lock
DynamoRegistry.get_detox_locks = get_detox_locks
