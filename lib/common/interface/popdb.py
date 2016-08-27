import logging
import time
import datetime
import pprint

from common.dataformat import DatasetReplica
from common.interface.access import AccessHistoryInterface
from common.interface.webservice import RESTService, GET, POST
import common.configuration as config

logger = logging.getLogger(__name__)

class PopDB(AccessHistoryInterface):

    def __init__(self, url_base = config.popdb.url_base):
        super(self.__class__, self).__init__()

        self._popdb_interface = RESTService(url_base)

    def get_local_accesses(self, site, date): #override
        if site.name.startswith('T0'):
            return []
        elif site.name.startswith('T1') and site.name.count('_') > 2:
            nameparts = site.name.split('_')
            sitename = '_'.join(nameparts[:3])
            service = 'popularity'
        elif site.name == 'T2_CH_CERN':
            sitename = site.name
            service = 'xrdpopularity'
        else:
            sitename = site.name
            service = 'popularity'

        datestr = date.strftime('%Y-%m-%d')
        result = self._make_request(service + '/DSStatInTimeWindow/', ['sitename=' + sitename, 'tstart=' + datestr, 'tstop=' + datestr])

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
        result = popdb._make_request('popularity/DSStatInTimeWindow/', args.options)

    pprint.pprint(result)
