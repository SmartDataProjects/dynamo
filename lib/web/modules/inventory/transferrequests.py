import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import InvalidRequest
from dynamo.dataformat import Dataset, Group
from dynamo.request.copy import CopyRequestManager
from dynamo.dataformat.request import Request
from dynamo.dataformat import Dataset

from dynamo.utils.interface.mysql import MySQL
from dynamo.history.history import HistoryDatabase

LOG = logging.getLogger(__name__)

class TransferRequestList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        
        self.copy_manager = CopyRequestManager()
        self.copy_manager.set_read_only()
        self.mysql_hist = HistoryDatabase(config.get('history', None))


    def run(self, caller, request, inventory):
        if 'request' not in request:
            return {'request':[]}

        req_id = int(request['request'])
        sql_line = 'select operation_id from phedex_requests as pr where pr.id = ' + str(req_id)
        LOG.info(sql_line)
        dbRequests = self.mysql_hist.db.query(sql_line)
        for line in dbRequests:
            req_id = int(line)
            break
            
        req_hash = self.copy_manager.get_requests(request_id=req_id)
        if req_id not in req_hash:
            return {'request':[]}
        
        req_obj = req_hash[req_id]
        request_by = {'requested_by':{'name':req_obj.user,'username':req_obj.user,'dn':req_obj.user_dn}}

        destinations = []
        sites = req_obj.find_sites(inventory)
        for site_obj in sites:
            node_json = []
            node_json.append({'se':site_obj.host,'name':site_obj.name,'id':site_obj.id,'desided_by':{'time_decided':req_obj.last_request,'decision':'y','dn':req_obj.user_dn} } )
            destinations.append(node_json)

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
                             "data":data_part,'requested_by':request_by,'destinations':destinations} ]}
                
# exported to __init__.py
export_data = {
    'transferrequests': TransferRequestList
}
