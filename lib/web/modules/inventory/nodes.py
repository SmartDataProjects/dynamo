import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import InvalidRequest
from dynamo.dataformat import Dataset, Group
from dynamo.request.copy import CopyRequestManager
from dynamo.dataformat.request import Request
from dynamo.dataformat import Dataset
from dynamo.dataformat import Site


LOG = logging.getLogger(__name__)

class NodesList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)


    def run(self, caller, request, inventory):
        node_array = []

        site_objs = []
        if 'node' in request:
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
        else:
            for site_name in inventory.sites:
                site_objs.append(inventory.sites[node_name])


        for siteObj in site_objs:
            kind = Site.storage_type_name(siteObj.storage_type)
            se = siteObj.host
            technology= ""
            siteid = siteObj.id
            hash_entry = {'kind':kind, 'se':se, 'technology':technology, 'name':siteObj.name, 'id':siteid}
            node_array.append(hash_entry)

        return {'node': node_array}
        
        
# exported to __init__.py
export_data = {
    'nodes': NodesList
}
