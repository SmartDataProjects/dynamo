import time
import logging
import fnmatch
import threading

from dynamo.dataformat import Site
from dynamo.source.siteinfo import SiteInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.interface.ssb import SiteStatusBoard

LOG = logging.getLogger(__name__)

def lfn_to_pfn(lfn, protocol, xml):

    if type(xml) is str:
        a = ET.parse(xml)
        root = a.getroot()
    else:
        root = xml

    for lfn_tag in root.findall('lfn-to-pfn'):
        path_match = lfn_tag.get('path-match')
        protocol_tag = lfn_tag.get('protocol')
        match = re.match(path_match, lfn)
        chain_protocol = lfn_tag.get('chain')
        result = lfn_tag.get('result')

        if match and protocol_tag == protocol:
            if chain_protocol is not None: 
                replacement = lfn_to_pfn(lfn, chain_protocol, root)
                pfn = result.replace(r'$1', replacement)
            else:
                pfn = result

                idx = 1
                while True:
                    try:
                        replacement = match.group(idx)
                        pfn = pfn.replace(r'$%d' % (idx), replacement)
                    except IndexError:
                        break

                    idx += 1

            return pfn

    return lfn

class PhEDExSiteInfoSource(SiteInfoSource):
    """SiteInfoSource for PhEDEx. Also use CMS Site Status Board for additional information."""

    def __init__(self, config):
        SiteInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.phedex)
        self._ssb = SiteStatusBoard(config.ssb)

        self.ssb_cache_lifetime = config.ssb_cache_lifetime
        self._ssb_cache_timestamp = 0
        self._caching_lock = threading.Lock()

        self._waitroom_sites = set()
        self._morgue_sites = set()

    def get_site(self, name): #override
        if self.exclude is not None:
            for pattern in self.exclude:
                if fnmatch.fnmatch(entry['name'], pattern):
                    LOG.info('get_site(%s)  %s is excluded by configuration.', name, name)
                    return None

        LOG.info('get_site(%s)  Fetching information of %s from PhEDEx', name, name)

        result = self._phedex.make_request('nodes', ['node=' + name])
        if len(result) == 0:
            return None

        entry = result[0]

        dummy_lfn = '/store/data/Run2010A/AlCaP0/ALCARECO/v1/000/135/809/48D7671F-B063-DF11-BDB5-0030487CD17C.root'
        pfn = lfn_to_pfn(dummy_lfn, 'srmv2', xmlfile)
        backend = pfn.replace(dummy_lfn, '')

        return Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = backend)

    def get_site_list(self): #override
        options = []

        if self.include is not None:
            options.extend('node=%s' % s for s in self.include)

        LOG.info('get_site_list  Fetching the list of nodes from PhEDEx')

        site_list = []

        for entry in self._phedex.make_request('nodes', options):
            if entry['name'].endswith('_Export') or entry['name'].endswith('_Buffer'):
                continue

            if self.exclude is not None:
                matched = False
                for pattern in self.exclude:
                    if fnmatch.fnmatch(entry['name'], pattern):
                        matched = True
                        break

                if matched:
                    continue

            site_list.append(Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology']))

        return site_list

    def get_site_status(self, site_name): #override
        with self._caching_lock:
            if time.time() > self._ssb_cache_timestamp + self.ssb_cache_lifetime:
                self._waitroom_sites = set()
                self._morgue_sites = set()
    
                latest_status = {}
    
                # get list of sites in waiting room (153) and morgue (199)
                for colid, stat, sitelist in [(153, Site.STAT_WAITROOM, self._waitroom_sites), (199, Site.STAT_MORGUE, self._morgue_sites)]:
                    result = self._ssb.make_request('getplotdata', 'columnid=%d&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1' % colid)
                    for entry in result:
                        site = entry['VOName']
                        
                        # entry['Time'] is UTC but we are only interested in relative times here
                        timestamp = time.mktime(time.strptime(entry['Time'], '%Y-%m-%dT%H:%M:%S'))
                        if site in latest_status and latest_status[site][0] > timestamp:
                            continue
        
                        if entry['Status'] == 'in':
                            latest_status[site] = (timestamp, stat)
                        else:
                            latest_status[site] = (timestamp, Site.STAT_READY)
    
                for site, (_, stat) in latest_status.items():
                    if stat == Site.STAT_WAITROOM:
                        self._waitroom_sites.add(site)
                    elif stat == Site.STAT_MORGUE:
                        self._morgue_sites.add(site)
    
                self._ssb_cache_timestamp = time.time()

        if site_name in self._waitroom_sites:
            return Site.STAT_WAITROOM
        elif site_name in self._morgue_sites:
            return Site.STAT_MORGUE
        else:
            return Site.STAT_READY
