#!/usr/bin/env python

import sys
import os
import logging
import time
import re
import fnmatch
import pprint

from argparse import ArgumentParser

parser = ArgumentParser(description = 'Dynamo registry user tools')
parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update|service|auth)')
parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = 'INFO', help = 'Logging level.')

args = parser.parse_args()
sys.argv = []

# Need to setup logging before loading other modules
log_level = getattr(logging, args.log_level.upper())

logging.basicConfig(level = log_level)

logger = logging.getLogger(__name__)

from common.interface.mysql import MySQL

store = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-dynamo', db = 'dynamoregister')

if args.command[0] == 'update':
    logger.info('Synchronizing the user list to SiteDB.')

    from common.interface.sitedb import SiteDB
    sitedb = SiteDB()

    domain_id = store.query('SELECT `id` FROM `domains` WHERE `name` = \'cern.ch\'')[0]

    query = 'INSERT INTO `users` (`name`, `domain_id`, `email`, `dn`) VALUES (%s, ' + str(domain_id) +', %s, %s) ON DUPLICATE KEY UPDATE `email` = `email`, `dn` = `dn`'

    names = []

    for user_info in sitedb._make_request('people'):
        name = user_info[0]
        email = user_info[1]
        dn = user_info[4]

        if dn is None:
            continue

        names.append(name)
 
        store.query(query, name, email, dn)

    store.delete_not_in('users', 'name', names)

elif args.command[0] == 'service':
    if args.command[1] == 'add':
        store.query('INSERT INTO `services` (`name`) VALUES (%s)', args.command[2])
    elif args.command[1] == 'remove':
        store.query('DELETE FROM `services` WHERE `name` = %s', args.command[2])
    elif args.command[1] == 'list':
        pprint.pprint(store.query('SELECT * FROM `services`'))

elif args.command[0] == 'auth':
    if args.command[1] == 'add':
        user = args.command[2]
        service = args.command[3]
        store.query('INSERT INTO `authorized_users` (`user_id`, `service_id`) SELECT u.`id`, s.`id` FROM `users` AS u, `services` AS s WHERE u.`name` = %s AND s.`name` = %s', user, service)
    elif args.command[1] == 'remove':
        user = args.command[2]
        service = args.command[3]
        store.query('DELETE FROM `authorized_users` WHERE (`user_id`, `service_id`) IN (SELECT u.`id`, s.`id` FROM `users` AS u, `services` AS s WHERE u.`name` = %s AND s.`name` = %s)', user, service)
    elif args.command[1] == 'list':
        pprint.pprint(store.query('SELECT * FROM `authorized_users`'))
