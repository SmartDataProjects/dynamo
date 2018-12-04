import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import InvalidRequest
from dynamo.dataformat import Dataset, Group
from dynamo.request.copy import CopyRequestManager
from dynamo.dataformat.request import Request
from dynamo.dataformat import Dataset


LOG = logging.getLogger(__name__)

class Lfn2PfnModule(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)


    def run(self, caller, request, inventory):
        if 'protocol' not in request:
            return {'mapping': []}
        if 'node' not in request:
            return {'mapping': []}
        if 'lfn' not in request:
            return {'mapping': []}

        LOG.info(request)

        protocol = request['protocol']
        lfn_name = request['lfn']
        custodial = None

        site_objs = []
        node_name = request['node']
        if '*' in node_name:
            pattern = re.compile(fnmatch.translate(node_name))
            for site_name in inventory.sites:
                if pattern.match(site_name):
                    site_objs.append(inventory.sites[site_name])
                    
        else:
            try:
                site_objs.append(inventory.sites[node_name])
            except KeyError:
                pass

        mapping = []
        for siteObj in site_objs:
            pfn_name = siteObj.to_pfn(lfn_name,protocol)
            if pfn_name is None:
                pfn_name = siteObj.to_pfn(lfn_name,'gfal2')

            destination = None
            space_token = None
            mapping.append({'protocol':protocol, 'custodial':custodial, 'destination':destination,
                                'space_token':space_token,'node':node_name, 'lfn':lfn_name, 'pfn':pfn_name })
        return {'mapping': mapping}
        
        
# exported to __init__.py
export_data = {
    'lfn2pfn': Lfn2PfnModule
}
