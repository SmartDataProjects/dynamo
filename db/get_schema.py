import sys
import re
import getpass
import MySQLdb
from argparse import ArgumentParser
from ConfigParser import ConfigParser, NoOptionError

parser = ArgumentParser(description = 'Write CREATE TABLE statements to stdout.', add_help = False)
parser.add_argument('db', metavar = 'DB', help = 'DB name.')
parser.add_argument('tables', metavar = 'TABLE', nargs = '+', help = 'Table name(s).')
parser.add_argument('--host', '-h', metavar = 'HOST', dest = 'host', default = 'localhost', help = 'DB host.')
parser.add_argument('--user', '-u', metavar = 'USER', dest = 'user', default = '', help = 'DB user.')
parser.add_argument('--passwd', '-p', metavar = 'PASSWD', dest = 'passwd', default = '', help = 'DB password.')
parser.add_argument('--defaults-group-suffix', metavar = 'SUFFIX', dest = 'defaults_suffix', default = '', help = 'Defaults file block suffix.')
parser.add_argument('--defaults-file', metavar = 'PATH', dest = 'defaults_file', default = '/etc/my.cnf', help = 'Defaults file.')
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

params['db'] = args.db

conn = MySQLdb.connect(**params)

cursor = conn.cursor()

for table in args.tables:
    cursor.execute('SHOW CREATE TABLE `%s`' % table)
    rows = cursor.fetchall()
    print re.sub(' AUTO_INCREMENT=[0-9]*', '', rows[0][1]) + ';'
    if len(args.tables) > 1:
        print ''
