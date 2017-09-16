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

    known_users = {}
    for name, email, dn in store.xquery('SELECT `name`, `email`, `dn` FROM `users` WHERE `domain_id` = %s', domain_id):
        known_users[name] = (email, dn)

    updated_users = []

    sitedb_users = {}
    sitedb.get_user_list(sitedb_users)

    for name, email, dn in sitedb_users.itervalues():
        try:
            known_user = known_users.pop(name)
        except KeyError:
            updated_users.append((name, domain_id, email, dn))
        else:
            if known_user != (email, dn):
                updated_users.append((name, domain_id, email, dn))

    store.insert_many('users', ('name', 'domain_id', 'email', 'dn'), None, updated_users, do_update = True)

    if len(known_users) != 0:
        store.delete_in('users', 'name', known_users.iterkeys())

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
