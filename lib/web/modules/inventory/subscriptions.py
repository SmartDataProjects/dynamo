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


    def run(self, caller, request, inventory):

        if 'suspended' in request:
            if request['suspended'] == 'y':
                return {'subscription' : []}
        if 'move' in request:
            if request['move'] == 'y':
                return {'subscription' : []}

        data_blocks = {}
        if 'dataset' in request:
            dset_name = request['dataset']
            if Dataset.name_pattern.match(dset_name) is None:
                raise InvalidRequest('Invalid dataset name %s' % dset_name)
            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for thename,dset_obj in inventory.datasets.iteritems():
                    data_blocks[dset_obj] = []
                    if pattern.match(thename):
                        for blk_obj in data_obj.blocks:
                            data_blocks[dset_obj].append(blk_obj)
            elif dset_name in inventory.datasets:
                data_obj = inventory.datasets[dset_name]
                data_blocks[dset_obj] = []
                for blk_obj in data_obj.blocks:
                    data_blocks.append(blk_obj)

        elif 'block' in request:
            block_full_name = request['block']
            try:
                dset_name, _, block_name = block_full_name.partition('#')
            except:
                raise InvalidRequest('Invalid block name %s' % block_full_name)
            if Dataset.name_pattern.match(dset_name) is None:
                raise InvalidRequest('Invalid dataset name %s' % dset_name)

            datasets = []
            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for dset_obj in inventory.datasets.itervalues():
                    if pattern.match(dset_obj.name) is not None:
                        datasets.append(dset_obj)
            elif dset_name in inventory.datasets:
                datasets.append(inventory.datasets[dset_name])

            if '*' in block_name:
                pattern = re.compile(fnmatch.translate(block_name))
            else:
                pattern = None

            for dset_obj in datasets:
                data_blocks[dset_obj] = []
                for block_obj in dset_obj.blocks:
                    if pattern is None:
                        if block_obj.real_name() == block_name:
                            data_blocks[dset_obj].append(block_obj)
                            break
                    else:
                        if pattern.match(block_obj.real_name()) is not None:
                            data_blocks[dset_obj].append(block_obj)

        data_block_reps = {}
        for dset_obj in data_blocks:
            nodepat = None
            if 'node' in request:
                nodepat = re.compile(fnmatch.translate(request['node']))

            for dset_repl in dset_obj.replicas:
                if nodepat is not None:
                    if nodepat.match(data_repl.site.name) is None:
                        continue
                if 'se' in request:
                    if data_repl.site.host != request['se']:
                        continue
                        
                data_block_reps[dset_repl] = []

                for block_repl in dset_repl.block_replicas:
                    if 'group' in request:
                        if block_repl.group.name != request['group']:
                            continue
                    data_block_reps[dset_repl].append(block_repl)

        dsetline = []
        for dset_obj in data_blocks:
            dsetrepline = []
            for dset_repl in dset_obj.replicas:
                if dset_repl not in data_block_reps:
                    continue
                if len(data_block_reps[dset_repl]) < 1:
                    continue
                if len(data_block_reps[dset_repl]) != len(dset_obj.blocks):
                    continue
                dsetrepline.append({'node': dset_repl.site.name})

            if len(dsetrepline) < 1:
                continue
            dsetline.append({'name':dset_obj.name, 'id': dset_obj.id, 'bytes': dset_obj.size,
                             'files': dset_obj.num_files, 'is_open': 'y', 'subscription': dsetrepline})


        for dset_obj in data_blocks:
            dsetrepline = []
            block_to_lines = {}
            for dset_repl in dset_obj.replicas:
                if dset_repl not in data_block_reps:
                    continue
                if len(data_block_reps[dset_repl]) < 1:
                    continue
                if len(data_block_reps[dset_repl]) == len(dset_obj.blocks):
                    continue
                
                for block_repl in data_block_reps[dset_repl]:
                    block_obj = block_repl.block
                    if block_obj not in block_to_lines:
                        block_to_lines[block_obj] = []
                    block_to_lines[block_obj].append({'node': block_repl.site.name, 'id': block_repl.site.id,
                                                      'se': block_repl.site.host, 'level': 'block', 
                                                      'node_files': block_repl.num_files, 'node_bytes': block_repl.size,
                                                      'group': block_repl.group.name,'suspended': 'n'})

            blockline = []
            for block_obj in block_to_lines:
                blockline.append({'name': block_obj.name, 'subscription': block_to_lines[block_obj]})

            dsetline.append({'name':dset_obj.name, 'id': dset_obj.id, 'bytes': dset_obj.size,
                             'files': dset_obj.num_files, 'is_open': 'y', 'block': blockline})

            
        return {'subsriptions': dsetline}
        
# exported to __init__.py
export_data = {
    'subscriptions': SubscriptionsList
}
