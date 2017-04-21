import logging
import time
import datetime
import threading
import pprint

from common.interface.access import AccessHistory
from common.interface.webservice import RESTService, GET, POST
import common.configuration as config
from common.misc import parallel_exec

logger = logging.getLogger(__name__)

class PopDB(AccessHistory):
    """
    Interface to CMS Popularity DB. This is intended as a plugin to the DemandManager.
    """

    def __init__(self, url_base = config.popdb.url_base):
        super(self.__class__, self).__init__()

        self._popdb_interface = RESTService(url_base, use_cache = True)

    def update(self, inventory): #override
        records = inventory.store.load_replica_accesses(inventory.sites.values(), inventory.datasets.values())
        self._last_update = records[0]
        full_access_list = records[1]

        start_time = max(self._last_update, (time.time() - 3600 * 24 * config.popdb.max_back_query))
        logger.info('Updating dataset access info from %s to %s', time.strftime('%Y-%m-%d', time.gmtime(start_time)), time.strftime('%Y-%m-%d', time.gmtime()))

        lock = threading.Lock()
        access_list = {}

        def query_popdb(site, date):
            if site.name.startswith('T0'):
                return []
            elif site.name.startswith('T1') and site.name.count('_') > 2:
                nameparts = site.name.split('_')
                sitename = '_'.join(nameparts[:3])
                service = 'popularity/DSStatInTimeWindow/' # wtf
            elif site.name == 'T2_CH_CERN':
                sitename = site.name
                service = 'xrdpopularity/DSStatInTimeWindow'
            else:
                sitename = site.name
                service = 'popularity/DSStatInTimeWindow/'
    
            datestr = date.strftime('%Y-%m-%d')
            result = self._make_request(service, ['sitename=' + sitename, 'tstart=' + datestr, 'tstop=' + datestr])
            
            with lock:
                for ds_entry in result:
                    try:
                        dataset = inventory.datasets[ds_entry['COLLNAME']]
                    except KeyError:
                        continue

                    if dataset.replicas is None:
                        continue

                    replica = dataset.find_replica(site)
                    if replica is None:
                        continue

                    if replica not in full_access_list:
                        full_access_list[replica] = {}

                    if replica not in access_list:
                        access_list[replica] = {}

                    full_access_list[replica][date] = int(ds_entry['NACC'])
                    access_list[replica][date] = (int(ds_entry['NACC']), float(ds_entry['TOTCPU']))

        utctoday = datetime.date(*time.gmtime()[:3])

        sitedates = []
        for site in inventory.sites.values():
            date = datetime.date(*time.gmtime(start_time)[:3])
            while date <= utctoday: # get records up to today
                sitedates.append((site, date))
                date += datetime.timedelta(1) # one day

        parallel_exec(query_popdb, sitedates)

        inventory.store.save_replica_accesses(access_list)

        self._last_update = time.time()

        self._compute(full_access_list)

    def get_xrootd_accesses(self, site, date): #override                                                     
        service = 'xrdpopularity/DSStatInTimeWindow'
        datestr = date.strftime('%Y-%m-%d')
        result = self._make_request(service, ['sitename=' + sitename, 
                                              'tstart=' + datestr, 'tstop=' + datestr])
        accesses = []
        for ds_entry in result:
            access = DatasetReplica.Access(int(ds_entry['NACC']), float(ds_entry['TOTCPU']))
            accesses.append((ds_entry['COLLNAME'], access))
            
        return accesses

    def _make_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single popdb request call. Returns the result json interpreted as a python dict.
        """

        resp = self._popdb_interface.make_request(resource, options = options, method = method, format = format)
        #print resp

        result = resp['DATA']
        del resp

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(pprint.pformat(result))

        return result


if __name__== '__main__':
    import sys
    from argparse import ArgumentParser
    from common.dataformat import Site

    parser = ArgumentParser(description = 'PopDB interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute. (dsstat sitename=* tstart=* tstop=*)')
    parser.add_argument('options', metavar = 'EXPR', nargs = '*', default = [], help = 'Option string as passed to PopDB.')

    args = parser.parse_args()
    sys.argv = []

    popdb = PopDB()

    if args.command == 'dsstat':
        service = 'xrdpopularity'
        for opt in args.options:
            if 'sitename=' in opt:
                sitename = opt[opt.find('=') + 1:]
                if sitename == 'T2_CH_CERN':
                    service = 'xrdpopularity'

        result = popdb._make_request(service + '/DSStatInTimeWindow', args.options)

    pprint.pprint(result)
