import json
import logging
import time
import pprint

from common.dataformat import DatasetReplica
from common.interface.access import AccessHistoryInterface
from common.interface.webservice import RESTService, GET, POST
from common.misc import unicode2str
import common.configuration as config

logger = logging.getLogger(__name__)

class PopDB(AccessHistoryInterface):

    def __init__(self, url_base = config.popdb.url_base):
        super(self.__class__, self).__init__()

        self._popdb_interface = RESTService(url_base)

    def set_access_history(self, site, time_start, time_end): #override
        s = time.localtime(time_start) # timestamps passed to the function are already in UTC -> use localtime (at Greenwich)
        e = time.localtime(time_end)

        start_timestamp = time.mktime((s.tm_year, s.tm_mon, s.tm_mday, 0, 0, 0, s.tm_wday, s.tm_yday, s.tm_isdst))
        end_timestamp = time.mktime((e.tm_year, e.tm_mon, e.tm_mday, 0, 0, 0, e.tm_wday, e.tm_yday, e.tm_isdst))

        tstart = '%4d-%02d-%02d' % (s.tm_year, s.tm_mon, s.tm_mday)
        tstop = '%4d-%02d-%02d' % (e.tm_year, e.tm_mon, e.tm_mday)

        if site.name.startswith('T1') and site.name.count('_') > 2:
            nameparts = site.name.split('_')
            sitename = '_'.join(nameparts[:3])
        else:
            sitename = site.name

        print ['sitename=' + sitename, 'tstart=' + tstart, 'tstop=' + tstop]

        result = self._make_request('popularity/DSStatInTimeWindow/', ['sitename=' + sitename, 'tstart=' + tstart, 'tstop=' + tstop])

        for ds_entry in result:
            dataset = site.find_dataset(ds_entry['COLLNAME'])
            if dataset is None:
                continue

            replica = dataset.find_replica(site)

            endtimes = [a.time_end for a in replica.accesses[DatasetReplica.ACC_LOCAL]]
            if len(endtimes) != 0 and max(endtimes) >= start_timestamp:
                continue

            replica.accesses[DatasetReplica.ACC_LOCAL].append(DatasetReplica.Access(start_timestamp, end_timestamp, int(ds_entry['NACC'])))

    def _make_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single popdb request call. Returns the result json interpreted as a python dict.
        """

        resp = self._popdb_interface.make_request(resource, options = options, method = method, format = format)
        logger.info('PopDB returned a response of ' + str(len(resp)) + ' bytes.')

        result = json.loads(resp)['DATA']
        unicode2str(result)

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
        result = popdb._make_request('popularity/DSStatInTimeWindow/', args.options)

    pprint.pprint(result)
