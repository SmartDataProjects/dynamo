#!/usr/bin/env python

import sys
import time
from argparse import ArgumentParser

from common.interface.mysqlhistory import MySQLHistory

parser = ArgumentParser(description = 'SitesInfo')
parser.add_argument('--cycle', '-c', metavar = 'ID', dest = 'cycle', type = int, default = 0, help = 'Cycle number.')

args = parser.parse_args()
sys.argv = []

history = MySQLHistory()

history.acquire_lock()

try:
    if args.cycle == 0:
        cycle = history.get_latest_deletion_run(partition = 'AnalysisOps')
        print 'Cycle', cycle
    else:
        cycle = args.cycle

    timestamp = history.get_run_timestamp(cycle)
    sites_info = history.get_sites(cycle)
    sites_usage = history.get_deletion_decisions(cycle)

    with open('/tmp/SitesInfo.txt', 'w') as output:
        output.write('#- %s\n' % time.strftime('%Y-%m-%d %H:%M', time.localtime(timestamp)))
        output.write('#\n')
        output.write('#- S I T E S  I N F O R M A T I O N ----\n')
        output.write('#\n')
        output.write('#\n')
        output.write('#- DDM Partition: AnalysisOps -\n')
        output.write('#\n')
        output.write('#  Active Quota[TB] Taken[TB] LastCopy[TB] SiteName\n')
        output.write('#------------------------------------------------------\n')

        quota_total = 0
        used_total = 0.
        protect_total = 0.

        num_t2 = 0
        quota_t2total = 0
        used_t2total = 0.
        protect_t2total = 0.

        for site in sorted(sites_info):
            active, status, quota = sites_info[site]
            try:
                protect, delete, keep = sites_usage[site]
            except KeyError:
                protect, delete, keep = (0., 0., 0.)

            used = protect + delete + keep
            
            output.write("   %-6d %-9d %-9.0f %-12.0f %-20s \n" % \
                (active, quota, used, protect, site))

            quota_total += quota
            used_total += used
            protect_total += protect

            if site.startswith('T2_'):
                num_t2 += 1
                quota_t2total += quota
                used_t2total += used
                protect_t2total += protect

        output.write('#------------------------------------------------------\n')

        output.write('#  %-6d %-9d %-9d %-12d %-20s \n' % \
            (len(sites_info), quota_total, used_total, protect_total, 'Total T2s+T1s'))

        if quota_total > 0:
            used_fraction = used_total / quota_total
            protect_fraction = protect_total / quota_total
        else:
            used_fraction = 1.
            protect_fraction = 1.

        output.write('#  %-6s %-9s %-4.1f%%     %-4.1f%%\n' % \
            (' ', ' ', used_fraction * 100., protect_fraction * 100.))
        
        output.write('#\n')

        output.write('#  %-6d %-9d %-9d %-12d %-20s \n' % \
            (num_t2, quota_t2total, used_t2total, protect_t2total, 'Total T2s'))

        if quota_t2total > 0:
            used_fraction = used_t2total / quota_t2total
            protect_fraction = protect_t2total / quota_t2total
        else:
            used_fraction = 1.
            protect_fraction = 1.

        output.write('#  %-6s %-9s %-4.1f%%     %-4.1f%%\n' % \
            (' ', ' ', used_fraction * 100., protect_fraction * 100.))

        output.write('#------------------------------------------------------\n')
        
finally:
    history.release_lock()
