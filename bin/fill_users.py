#!/usr/bin/env python

from common.interface.sitedb import SiteDB
from common.interface.mysql import MySQL

sitedb = SiteDB()
store = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-dynamo', db = 'dynamoregister')

for user_info in sitedb._make_request('people'):
    name = user_info[0]
    email = user_info[1]
    dn = user_info[4]

    if dn is None:
        continue

    store.query('INSERT INTO `users` (`name`, `email`, `dn`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `email` = `email`, `dn` = `dn`', name, email, dn)
