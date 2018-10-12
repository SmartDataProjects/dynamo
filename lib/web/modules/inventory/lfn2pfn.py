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
        node_name = request['node']
        lfn_name = request['lfn']
        custodial = 'Null'

        if node_name not in inventory.sites:
            return {'mapping': []}

        siteObj = inventory.sites[node_name]
        pfn_name = siteObj.to_pfn(lfn_name,protocol)
        if pfn_name is None:
            pfn_name = siteObj.to_pfn(lfn_name,'gfal2')

        destination = 'Null'
        space_token = 'Null'
        return {'mapping': [{'protocol':protocol, 'custodial':custodial, 'destination':destination, 
                             'space_token':space_token,'node':node_name, 'lfn':lfn_name, 'pfn':pfn_name }] }
        
        
# exported to __init__.py
export_data = {
    'lfn2pfn': Lfn2PfnModule
}
