import fnmatch
import re
import logging
import datetime

from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from common.dataformat import Site
import common.configuration as config

logger = logging.getLogger(__name__)

class SiteDB(SiteInfoSourceInterface):
    def __init__(self):
        self._interface = RESTService(config.sitedb.url_base)

    def get_site_list(self, sites, filt = '*'): #override
        """
        Fill the list of sites with sites that match the wildcard name.
        Arguments:
          sites: the name->site dict to be filled. Information of the sites already in the list will be updated.
          filt: a wildcard string or a list of wildcard strings.
        """

        if type(filt) is list:
            namepatterns = [re.compile(fnmatch.translate(f)) for f in filt]
        else:
            namepatterns = [re.compile(fnmatch.translate(filt))]

        result = self._make_request('site-names') # [[type, long name, T*_*_*]]
        site_names = dict([(entry[2], entry[1]) for entry in result if entry[0] == 'cms'])

        result = self._make_request('site-resources') # [[long name, CE/SE, fqdn, is_primary]]
        ses = dict([(entry[0], entry[2]) for entry in result if entry[1] == 'SE'])

        result = self._make_request('resource-pledges') # [[long name, timestamp, year, cpu, disk, tape, local]]
        longnames = site_names.values()

        latest_pledges = {}
        for longname, timestamp, year, cpu, disk, tape, local in result:
            if longname not in longnames:
                continue

            if cpu == 0.:
                continue

            if longname not in latest_pledges or timestamp > latest_pledges[longname][0]:
                latest_pledges[longname] = (timestamp, cpu, disk)

        for name, longname in site_names.items():
            for pat in namepatterns:
                if pat.match(name):
                    break
            else:
                # no match found
                continue

            if name not in sites:
                try:
                    se = ses[longname]
                except KeyError:
                    logger.info('No SE host name found in SiteDB for %s', name)
                    se = ''

                try:
                    cpu, storage = latest_pledges[longname][1:3]
                except KeyError:
                    if name.endswith('_Disk'):
                        try:
                            cpu, storage = latest_pledges[site_names[name.replace('_Disk', '')]][1:3]
                        except KeyError:
                            logger.info('No resource pledge found in SiteDB for %s', name.replace('_Disk', ''))
                            cpu = 0.
                            storage = 0.
                    else:
                        logger.info('No resource pledge found in SiteDB for %s', name)
                        cpu = 0.
                        storage = 0.

                site = Site(name, host = se, storage_type = Site.TYPE_UNKNOWN, storage = storage, cpu = cpu)

                sites[name] = site

    def _make_request(self, resource, options = []):
        """
        Make a single API call to SiteDB, strip the "header" and return the body JSON.
        """

        resp = self._interface.make_request(resource, options)

        return resp['result']


if __name__ == '__main__':

    import sys
    import pprint
    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'SiteDB interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '*', default = [], help = 'Option string as passed to PhEDEx datasvc.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    interface = SiteDB()

    if args.command == 'get':
        sites = {}
        interface.get_site_list(sites, filt = config.inventory.included_sites)

        with open('/tmp/sitequery.sql', 'w') as out:
            for site in sites.values():
                if site in config.inventory.excluded_sites:
                    continue

                out.write('UPDATE `sites` SET storage = %f, cpu = %f WHERE `name` LIKE \'%s\';\n' % (site.storage, site.cpu, site.name))
