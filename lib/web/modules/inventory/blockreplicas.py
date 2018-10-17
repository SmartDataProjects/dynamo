import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.dataformat import Dataset, Group

LOG = logging.getLogger(__name__)

class ListBlockReplicas(WebModule):
    """
    block relicas listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

    def run(self, caller, request, inventory):
        dset_name = ''
        block_name = ''
        if 'block' in request:
            dset_name = (request['block'].split('#'))[0]
            block_name = (request['block'].split('#'))[1]
        elif 'dataset' in request:
            dset_name = request['dataset']
        else:
            return []
        
        # collect information from the inventory and registry according to the requests
        datasets = []
        pattern = re.compile(fnmatch.translate(dset_name))
        if '*' in dset_name:
            for thename in inventory.datasets.iterkeys():
                if pattern.match(thename):
                    datasets.append(inventory.datasets[thename])
        else:
            if dset_name in inventory.datasets:
                datasets.append(inventory.datasets[dset_name])
        

        
        blocks = {}
        blockreps = {}
        if 'node' in request:
            nodepat = re.compile(fnmatch.translate(request['node']))
        if '*' in block_name:
            blockpat = re.compile(fnmatch.translate(block_name))
        for dset_obj in datasets:
            blocks[dset_obj] = []
            for block_obj in dset_obj.blocks:
                if '*' in block_name:
                    if not blockpat.match(block_obj.real_name()):
                        continue
                else:
                    if block_name != '' and block_name != block_obj.real_name():
                        continue

                blocks[dset_obj].append(block_obj)
                blockreps[block_obj] = []
                for blockrep_obj in block_obj.replicas:
                    if 'node' in request:
                        site_name = blockrep_obj.site.name
                        if '*' in request['node']:
                            if not nodepat.match(site_name):
                                continue
                        else:
                            if site_name != request['node']:
                                continue
                
                    if 'complete' in request:
                        if request['complete'] == 'y':
                            if not blockrep_obj.is_complete():
                                continue
                        if request['complete'] == 'n':
                            if blockrep_obj.is_complete():
                                continue

                    if 'group' in request:
                        if request['group'] != blockrep_obj.group.name:
                            continue

                    if 'update_since' in request:
                        update_since = int(request['update_since'])
                        if update_since > blockrep_obj.last_update:
                            continue

                    if 'create_since' in request:
                        update_since = int(request['create_since'])
                        if create_since > blockrep_obj.last_update:
                            continue
                    blockreps[block_obj].append(blockrep_obj)
           
        
        response = []
        
        for dset_obj in blocks:
            for block_obj in blocks[dset_obj]:
                repline = []
                for blkrep in blockreps[block_obj]:
                    if blkrep.group is Group.null_group:
                        subscribed = 'n'
                    else:
                        subscribed = 'y'

                    rephash = {'bytes': blkrep.size, 'node': blkrep.site.name, 'files': blkrep.num_files, 'node_id': blkrep.site.id, 
                               'se': blkrep.site.host, 'complete': self.crt(blkrep.is_complete()), 
                               'time_create': blkrep.last_update, 'time_update': blkrep.last_update,
                               'group': blkrep.group.name, 'custodial': self.crt(blkrep.is_custodial),
                               'subscribed': subscribed}
                    repline.append(rephash)
                if len(repline) < 1 : continue

                line = {'name': block_obj.full_name(), 'files': block_obj.num_files, 'bytes': block_obj.size, 
                        'is_open': self.crt(block_obj.is_open), 'id': block_obj.id, 'replica': repline }
                response.append(line)
        
        return {'block': response}

    def crt(self,boolval):
        if boolval == True: return 'y'
        return 'n'


# exported to __init__.py
export_data = {
    'blockreplicas': ListBlockReplicas
}
