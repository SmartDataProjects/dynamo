import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.dataformat import Dataset, Group

LOG = logging.getLogger(__name__)

class ListData(WebModule):
    """
    data listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

    def run(self, caller, request, inventory):
        if 'block' not in request:
            return {"dbs":[]}
        dset_name,block_name = (request['block'].split('#'))
        
        dset_obj = inventory.datasets[dset_name]
        block_obj = None
        for block_obj in dset_obj.blocks:
            if block_name == block_obj.real_name():
                break

        if block_obj is None:
            return {"dbs":[]}

        files_json = []
        for blockrep_obj in block_obj.replicas:
            site_name = blockrep_obj.site.name

            all_files = block_obj.files
            if not blockrep_obj.is_complete():
                all_files = blockrep_obj.files()
            for file_obj in all_files:
                cksum = 'alde32:' + str(file_obj.checksum[1]) + ',cksum:' + str(file_obj.checksum[0])
                file_hash = {'checksum': cksum,'node':site_name,'lfn':file_obj.lfn,
                             'time_create':blockrep_obj.last_update,'size':file_obj.size}
                
                files_json.append(file_hash)
            
        block_open = 'n'
        if block_obj.is_open:
            block_open = 'y'
            
        block_hash = [{'time_update':block_obj.last_update, 'bytes':block_obj.size, 'files':block_obj.num_files,
                       'name':block_obj.full_name(), 'is_open':block_open,'time_create':block_obj.last_update,
                       'file':files_json}]
        dset_hash = {"dataset":[{'time_update':None,'is_transient':'n','is_open':'y', 'name':dset_name, 'block':block_hash}]}

        return {"dbs":[dset_hash]}
                

# exported to __init__.py
export_data = {
    'data': ListData
}
