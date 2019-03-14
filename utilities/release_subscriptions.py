#!/usr/bin/env python

import sys
from argparse import ArgumentParser

parser = ArgumentParser(description = 'Unhold subscriptions')

parser.add_argument('--site', '-s', metavar = 'SITE', dest = 'site', help = 'Site name.')
parser.add_argument('--reason', '-r', metavar = 'REASON', dest = 'reason', help = 'Hold reason.')
parser.add_argument('--id', '-i', metavar = 'ID', dest = 'ids', nargs = '+', type = int, help = 'Subscription ids.')

args = parser.parse_args()
sys.argv = []

if args.site is None and args.reason is None and args.ids is None:
    sys.stderr.write('Cannot release all subscriptions.')
    sys.exit(1)

from dynamo.core.executable import inventory, authorized
from dynamo.fileop.rlfsm import RLFSM

rlfsm = RLFSM()

if not authorized:
    print "Using as read-only."
    rlfsm.set_read_only()

subscriptions = rlfsm.get_subscriptions(inventory, op = 'transfer', status = ['held'])

num_released = 0

for subscription in subscriptions:
    if args.ids is not None and subscription.id not in args.ids:
        continue

    if args.reason is not None and subscription.hold_reason != args.reason:
        continue

    rlfsm.release_subscription(subscription)

    num_released += 1

print 'Released %d subscriptions.' % num_released
