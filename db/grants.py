import os
import sys
import json
import getpass
import MySQLdb

passwd = getpass.getpass('Enter password for MySQL root:')

thisdir = os.path.dirname(os.path.realpath(__file__))

with open(thisdir + '/grants.json') as source:
    config = json.load(source)

db = MySQLdb.connect(user = 'root', passwd = passwd, host = 'localhost', db = 'mysql')

cursor = db.cursor()

users = set()
for user, userconf in config.items():
    for host in userconf['hosts']:
        users.add((user, host))

print users

cursor.execute('SELECT `User`, `Host` FROM `mysql`.`user`')
in_db = set(cursor.fetchall())

for user, host in (users - in_db):
    print 'Creating user \'%s\'@\'%s\'' % (user, host)
    db.query('CREATE USER \'%s\'@\'%s\' IDENTIFIED BY \'%s\'' % (user, host, config[user]['passwd']))

db.commit()

print 'Updating table grants.'

for user, host in users:
    cursor.execute('DELETE FROM `mysql`.`tables_priv` WHERE `User` = %s AND `Host` = %s', (user, host))

    for grant in config[user]['grants']:
        if len(grant) == 2:
            db.query('GRANT %s ON `%s`.* TO \'%s\'@\'%s\'' % (grant[0], grant[1], user, host))
        elif len(grant) == 2:
            db.query('GRANT %s ON `%s`.`%s` TO \'%s\'@\'%s\'' % (grant[0], grant[1], grant[2], user, host))

    db.commit()

print 'Done.\n'

for user, host in sorted(users):
    print 'Granted to %s@%s:' % (user, host)
    cursor.execute('SHOW GRANTS FOR \'%s\'@\'%s\'' % (user, host))
    for row in cursor.fetchall()[1:]:
        print row[0].replace('GRANT ', '').replace(' TO \'%s\'@\'%s\'' % (user, host), '')

    print ''
