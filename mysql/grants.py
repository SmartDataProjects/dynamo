import os
import sys
import json
import MySQLdb
from argparse import ArgumentParser

parser = ArgumentParser(description = 'Write CREATE TABLE statements to stdout.', add_help = False)
parser.add_argument('--host', '-h', metavar = 'HOST', dest = 'host', default = 'localhost', help = 'DB host.')
parser.add_argument('--user', '-u', metavar = 'USER', dest = 'user', default = '', help = 'DB user.')
parser.add_argument('--passwd', '-p', metavar = 'PASSWD', dest = 'passwd', default = '', help = 'DB password.')
parser.add_argument('--defaults-group-suffix', metavar = 'SUFFIX', dest = 'defaults_suffix', default = '', help = 'Defaults file block suffix.')
parser.add_argument('--defaults-file', metavar = 'PATH', dest = 'defaults_file', default = '/etc/my.cnf', help = 'Defaults file.')
parser.add_argument('--revoke', '-R', action = 'store_true', dest = 'revoke', help = 'Revoke all grants.')
parser.add_argument('--quiet', '-q', action = 'store_true', dest = 'quiet', help = "Don't print grant statements.")
parser.add_argument('--help', '-i', action = 'store_true', dest = 'help', help = 'Print this help.')

args = parser.parse_args()
sys.argv = []

if args.help:
    args.print_help()
    sys.exit(0)

params = {}
if args.defaults_file:
    params['read_default_file'] = args.defaults_file
    params['read_default_group'] = 'mysql' + args.defaults_suffix

for key in ['user', 'passwd', 'host']:
    if getattr(args, key):
        params[key] = getattr(args, key)

params['db'] = 'mysql'

db = MySQLdb.connect(**params)

thisdir = os.path.dirname(os.path.realpath(__file__))

with open(thisdir + '/grants.json') as source:
    config = json.load(source)

try:
    with open(thisdir + '/grants_ext.json') as source:
        ext_config = json.load(source)
except IOError:
    pass
else:
    for user, block in ext_config.items():
        if user is not in config:
            config[user] = block
        else:
            if 'passwd' in block:
                config[user]['passwd'] = block['passwd']
            if 'hosts' in block:
                config[user]['hosts'] = list(set(config[user]['hosts'] + block['hosts']))
            if 'grants' in block:
                config[user]['grants'].extend(block['grants'])

users = set()
for user, userconf in config.items():
    for host in userconf['hosts']:
        users.add((user, host))

cursor = db.cursor()

cursor.execute('SELECT `User`, `Host` FROM `mysql`.`user`')
in_db = set(cursor.fetchall())

if not args.revoke:
    for user, host in (users - in_db):
        print 'Creating user \'%s\'@\'%s\'' % (user, host)
        db.query('CREATE USER \'%s\'@\'%s\' IDENTIFIED BY \'%s\'' % (user, host, config[user]['passwd']))

db.commit()

print '-> Updating table grants.'

for user, host in users:
    cursor.execute('DELETE FROM `mysql`.`tables_priv` WHERE `User` = %s AND `Host` = %s', (user, host))

    if args.revoke:
        continue

    for grant in config[user]['grants']:
        if len(grant) == 2:
            db.query('GRANT %s ON `%s`.* TO \'%s\'@\'%s\'' % (grant[0], grant[1], user, host))
        elif len(grant) == 3:
            db.query('GRANT %s ON `%s`.`%s` TO \'%s\'@\'%s\'' % (grant[0], grant[1], grant[2], user, host))

    db.commit()

if args.revoke:
    for user, host in (users - in_db):
        print 'Deleting user \'%s\'@\'%s\'' % (user, host)
        db.query('DROP USER \'%s\'@\'%s\'')

print ' Done.\n'

if not args.quiet and not args.revoke:
    for user, host in sorted(users):
        print 'Granted to %s@%s:' % (user, host)
        cursor.execute('SHOW GRANTS FOR \'%s\'@\'%s\'' % (user, host))
        for row in cursor.fetchall()[1:]:
            print row[0].replace('GRANT ', '').replace(' TO \'%s\'@\'%s\'' % (user, host), '')
    
        print ''
