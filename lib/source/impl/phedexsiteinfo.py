"""
SiteInfoSource for PhEDEx. Also use CMS Site Status Board for additional information.
"""

import logging
import fnmatch

from source.siteinfo import SiteInfoSource
from common.interface.phedex import PhEDEx
from common.interface.webservice import RESTService

LOG = logging.getLogger(__name__)

class PhEDExSiteInfoSource(SiteInfoSource):
    def __init__(self, config):
        SiteInfoSource.__init__(self, config)

        self._phedex = PhEDEx()
        self._ssb = RESTService(config.ssb_url)

    def get_site(self, name): #override
        if self.exclude is not None:
            for pattern in self.exclude:
                if fnmatch.fnmatch(entry['name'], pattern):
                    LOG.info('get_site(%s)  %s is excluded by configuration.', name, name)
                    return None

        LOG.info('get_site(%s)  Fetching information of %s from PhEDEx', name, name)

        result = self._phedex.call('nodes', ['node=' + name])
        if len(result) == 0:
            return None

        entry = result[0]

        return Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology'])

    def get_site_list(self): #override
        options = []

        if self.include is not None:
            options.extend('node=%s' % s for s in self.include)

        LOG.info('get_site_list  Fetching the list of nodes from PhEDEx')

        site_list = []

        for entry in self._phedex.call('nodes', options):
            if self.exclude is not None:
                for pattern in self.exclude:
                    if fnmatch.fnmatch(entry['name'], pattern):
                        break
                else:
                    # no exclude pattern matched -> go ahead
                    pass

                continue

            site_list.append(Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology']))

        return site_list

    def set_site_properties(self, site): #override
        for site in sites.itervalues():
            site.status = Site.STAT_READY

        # get list of sites in waiting room (153) and morgue (199)
        for colid, stat in [(153, Site.STAT_WAITROOM), (199, Site.STAT_MORGUE)]:
            result = self._ssb.make_request('getplotdata', 'columnid=%d&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1' % colid)
            try:
                source = result['csvdata']
            except KeyError:
                logger.error('SSB parse error')
                return

            latest_timestamp = {}
    
            for entry in source:
                try:
                    site = sites[entry['VOName']]
                except KeyError:
                    continue
                
                # entry['Time'] is UTC but we are only interested in relative times here
                timestamp = time.mktime(time.strptime(entry['Time'], '%Y-%m-%dT%H:%M:%S'))
                if site in latest_timestamp and latest_timestamp[site] > timestamp:
                    continue

                latest_timestamp[site] = timestamp

                if entry['Status'] == 'in':
                    site.status = stat
                else:
                    site.status = Site.STAT_READY

