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

class SubscriptionsList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

    def get_replicas(self, item_name, inventory, data_blocks):
        dset_name = item_name
        block_name = None
        if '#' in item_name:
            dset_name, _, block_name = item_name.partition('#')

        if '*' in dset_name:
            pattern = re.compile(fnmatch.translate(dset_name))
            for thename,dset_obj in inventory.datasets.iteritems():
                if pattern.match(thename):
                    data_blocks[dset_obj] = []
        elif dset_name in inventory.datasets:
            dset_obj = inventory.datasets[dset_name]
            data_blocks[dset_obj] = []

        if block_name is not None:
            pattern = re.compile(fnmatch.translate(block_name))
            for dset_obj in data_blocks:
                for block_obj in dset_obj.blocks:
                    if pattern.match(block_obj.real_name()):
                        data_blocks[dset_obj].append(block_obj)

    def isright_group(self,obj_repl,request):
        if 'group' in request:
            if obj_repl.group.name != request['group']:
                return False
        return True

    def isright_site(self,obj_repl,request):
        if 'node' in request:
            nodepat = re.compile(fnmatch.translate(request['node']))
            if nodepat.match(obj_repl.site.name) is None:
                return False
        return True

    def get_percents(self,dset_repl):
        sum_size = 0.
        sum_files = 0.
        for block_repl in dset_repl.block_replicas:
            if block_repl.is_complete():
                sum_size = sum_size + block_repl.block.size
                sum_files = sum_files + block_repl.block.num_files

        perc_size = 188*sum_size/float(dset_repl.dataset.size)
        perc_files = 100*sum_files/float(dset_repl.dataset.num_files)
        return (perc_size,perc_files)

    def make_json(self,dset_obj,data_blocks,request,inventory):
        
        dset_hash = {'name':dset_obj.name, 'id': dset_obj.id, 'bytes': dset_obj.size,
                     'files': dset_obj.num_files, 'is_open': 'y'}

        blockline = []
        block_hash = {}
        for block_obj in data_blocks[dset_obj]:
            block_hash = {'bytes':block_obj.size, 'files':block_obj.num_files,
                          'id':block_obj.id, 'name':block_obj.full_name()}
            subsline = []
            for block_repl in block_obj.replicas:
                if not self.isright_group(block_repl,request):
                    continue
                if not self.isright_site(block_repl,request):
                    continue

                dset_repl = block_repl.site.find_dataset_replica(block_obj.dataset)
                if dset_repl.group is not None:
                    #skip dataset level
                    continue

                perc_size = 0
                perc_files = 0
                if block_repl.is_complete():
                    perc_size = 100
                    perc_files = 100
                subsline.append({'custodial':'n','group':block_repl.group.name,'level':'BLOCK',
                                 'node':block_repl.site.name,'node_bytes':block_repl.size(),
                                 'node_id':block_repl.site.id, "suspend_until": None,
                                 'percent_files':perc_files,'percent_bytes':perc_size,
                                 'time_update':block_repl.last_update})
            if len(subsline) > 0: 
                block_hash['subscription'] = subsline
        
        if 'subscription' in block_hash:
            dset_hash['block'] = block_hash

        subsline = []
        for dset_repl in dset_obj.replicas:
            if not self.isright_group(dset_repl,request):
                continue
            if not self.isright_site(dset_repl,request):
                continue

            custodial = 'n'
            if '_MSS' in dset_repl.site.name:
                custodial = 'y'
            (perc_size,perc_files) = self.get_percents(dset_repl)

            if dset_repl.group is not None:
                #dataset level
                subsline.append({'custodial':custodial,'group':dset_repl.group.name,
                                 'level':'DATASET','percent_bytes':perc_size,
                                 'percent_files':perc_files,
                                 'node':dset_repl.site.name,'node_bytes':dset_repl.size(),
                                 'node_id':dset_repl.site.id, "suspend_until": None,
                                 'time_update':dset_repl.last_block_created()})
            else:
                #block level subs
                if len(data_blocks[dset_obj]) == 0:
                    #screw it, still data level
                    subsline.append({'custodial':custodial,'group':dset_repl.group.name,
                                     'level':'DATASET','percent_bytes':perc_size,
                                     'percent_files':perc_files,
                                     'node':dset_repl.site.name,'node_bytes':dset_repl.size(),
                                     'node_id':dset_repl.site.id, "suspend_until": None,
                                     'time_update':dset_repl.last_block_created()})
                
        dset_hash['subscription'] = subsline
        return dset_hash

    def run(self, caller, request, inventory):

        if 'suspended' in request:
            if request['suspended'] == 'y':
                return {'dataset' : []}
        if 'move' in request:
            if request['move'] == 'y':
                return {'dataset' : []}

        data_blocks = {}
        if 'dataset' in request:
            dset_name = request['dataset']
            if '#' in dset_name:
                return {'dataset' : []}
            self.get_replicas(dset_name,inventory,data_blocks)

        elif 'block' in request:
            block_name = request['block']
            if '#' not in block_name:
                return {'dataset' : []}
            self.get_replicas(block_name,inventory,data_blocks)

        dset_hash_lines = []
        for dset_obj in data_blocks:
            dset_hash = self.make_json(dset_obj,data_blocks,request,inventory)
            dset_hash_lines.append(dset_hash)

        return {'dataset': dset_hash_lines }
        
# exported to __init__.py
export_data = {
    'subscriptions': SubscriptionsList
}
