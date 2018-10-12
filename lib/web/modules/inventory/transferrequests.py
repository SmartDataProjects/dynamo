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

class TransferRequestList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        
        self.copy_manager = CopyRequestManager()
        self.copy_manager.set_read_only()


    def run(self, caller, request, inventory):
        if 'request' not in request:
            return {'request':[]}

        req_id = int(request['request'])
        req_hash = self.copy_manager.get_requests(request_id=req_id)
        LOG.info(req_id)
        LOG.info(req_hash)
        if req_id not in req_hash:
            return {'request':[]}
        
        req_obj = req_hash[req_id]
        request_by = {'requested_by':{'name':req_obj.user,'username':req_obj.user,'dn':req_obj.user_dn}}

        destinations = []
        sites = req_obj.find_sites(inventory)
        for site_obj in sites:
            destinations.append({'desided_by':{'time_decided':req_obj.last_request,'decision':'y','dn':req_obj.user_dn},
                                 'se':site_obj.host,'name':site_obj.name,'id':site_obj.id})

        datasets = req_obj.find_items(inventory)
        all_bites = 0
        all_files = 0
        dset_part = []
        for dset_name in datasets:
            dset_obj = datasets[dset_name]
            dset_part.append({'bites':dset_obj.size, 'files':dset_obj.num_files,'name':dset_name,'id':dset_obj.id})
            all_bites = all_bites + dset_obj.size
            all_files = all_files + dset_obj.num_files
        
        data_part = {'bites':all_bites,'files':all_files,'time_create':req_obj.first_request,
                     'group':req_obj.group,'dbs':{'dataset':dset_part}}
        
        return {'request': [{"priority":"low","time_start":'null',"move":"n","id":req_id,
                             "data":data_part,'requested_by':request_by,'destinations':{'node':destinations}} ]}
                
# exported to __init__.py
export_data = {
    'transferrequests': TransferRequestList
}
