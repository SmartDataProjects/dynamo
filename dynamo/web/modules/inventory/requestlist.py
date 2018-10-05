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

class RequestList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        
        self.copy_manager = CopyRequestManager()
        self.copy_manager.set_read_only()


    def run(self, caller, request, inventory):
        if 'decision' in request:
            if request['decision'] != 'approved':
                return {'request': []}
        if 'approval' in request:
            if request['approval'] != 'approved':
                return {'request': []}
        if 'decided_by' in request:
            return {'request': []}


        # def get_requests(self, request_id = None, statuses = None, users = None, items = None, sites = None):
        #  __slots__ = ['request_id', 'user', 'user_dn', 'status', 'reject_reason', 'sites', 'items', 'actions']
        requested_by = None
        site_names = None
        item_names = None
        data_names = None
        if 'requested_by' in request:
            requested_by = [request['requested_by']]
        if 'node' in request:
            site_names = []
            nodepat = re.compile(fnmatch.translate(request['node']))
            for site in inventory.sites:
                if nodepat.match(site):
                    site_names.append(site)
            if len(site_names) < 1: site_names = None

        if 'dataset' in request:
            data_names = []
            dset_name = request['dataset']
            if Dataset.name_pattern.match(dset_name) is None:
                raise InvalidRequest('Invalid dataset name %s' % dset_name)

            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for thename in inventory.datasets.iterkeys():
                    if pattern.match(thename):
                        data_names.append(thename)
            elif dset_name in inventory.datasets:
                data_names.append(dset_name)
            if len(data_names) < 1: data_names = None

        
        if 'block' in request:
            item_names = []
            data_names = []
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
                data_names.append(dset_obj.name)
                for block_obj in dset_obj.blocks:
                    if pattern is None:
                        if block_obj.real_name() == block_name:
                            item_names.append(block_obj.full_name())
                    else:
                        if pattern.match(block_obj.real_name()) is not None:
                            item_names.append(block_obj.full_name())
            
            if len(item_names) < 1: 
                item_names = None
            if len(data_names) < 1:
                data_names = None


        #return {'request': item_names}
        
        #try:
        erequests = self.copy_manager.get_requests(users=requested_by, sites=site_names, items=item_names)
        if len(erequests) < 1:
            erequests = self.copy_manager.get_requests(users=requested_by, sites=site_names, items=data_names)

        response = []
        for reqid, req_obj in erequests.iteritems():
            if 'created_since' in request:
                created_since = int(request['created_since'])
                if created_since > req_obj.first_request:
                    continue
            if 'request' in request:
                reqstr = request['request']
                if '*' in reqstr:
                    pattern = re.compile(fnmatch.translate(reqstr))
                    if pattern.match(str(reqid)) is None:
                        continue
                elif reqid != int(reqstr):
                    continue
                            
            nodelines = []
            for site_name in req_obj.sites:
                if site_name not in inventory.sites:
                    continue
                site_obj = inventory.sites[site_name]
                nodelines.append({'id': site_obj.id, 'name': site_obj.name, 'se': site_obj.host,
                            'decision': 'approved', 'decided_by': req_obj.user, 
                            'time_decided': req_obj.first_request})
            response.append({'id': req_obj.request_id, 'type': 'xfer', 'approval': 'approved',
                             'requested_by': req_obj.user, 'time_create': req_obj.first_request,
                             'node': nodelines})

        return {'request': response}
                
# exported to __init__.py
export_data = {
    'requestlist': RequestList
}
