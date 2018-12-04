import fnmatch
import re
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import InvalidRequest
from dynamo.dataformat import Dataset, Group
from dynamo.request.copy import CopyRequestManager
from dynamo.request.deletion import DeletionRequestManager
from dynamo.dataformat.request import Request
from dynamo.dataformat import Dataset

from dynamo.utils.interface.mysql import MySQL
from dynamo.history.history import HistoryDatabase

LOG = logging.getLogger(__name__)

class RequestList(WebModule):
    """
    request listing
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        
        self.copy_manager = CopyRequestManager()
        self.copy_manager.set_read_only()
        self.dele_manager = DeletionRequestManager()
        self.dele_manager.set_read_only()

        self.mysql_hist = HistoryDatabase(config.get('history', None))

    def pro_requests(self,erequests,request,inventory):
        response = []
        for reqid, req_obj in erequests.iteritems():

            sql_line = 'select * from phedex_requests as pr where pr.operation_id = ' + str(reqid)
            if 'decision' in request:
                approved = 1
                decision = 'approved'
                if request['decision'] == 'pending':
                    approved = 0
                    decision = 'pending'
                sql_line += ' and approved = ' + str(approved)

            dbRequests = self.mysql_hist.db.query(sql_line)
            if len(dbRequests) < 1 :
                continue

            phedex_id = None
            req_type = None
            for line in dbRequests:
                #rep_array = line.split()
                phedex_id = int(line[0])
                req_type = line[1]
                if req_type == 'copy':
                    req_type = 'xfer'
                if req_type == 'deletion':
                    req_type = 'delete'
                break

            nodelines = []
            for site_name in req_obj.sites:
                if site_name not in inventory.sites:
                    continue
                site_obj = inventory.sites[site_name]
                nodelines.append({'id': site_obj.id, 'name': site_obj.name, 'se': site_obj.host,
                                  'decision': decision, 'decided_by': req_obj.user, 
                                  'time_decided': req_obj.first_request})
            response.append({'id': phedex_id, 'type': req_type, 'approval': decision,
                             'requested_by': req_obj.user, 'time_create': req_obj.first_request,
                             'node': nodelines})
        return response

    def run(self, caller, request, inventory):
        site_names = None
        data_names = None
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

            if '*' in dset_name:
                pattern = re.compile(fnmatch.translate(dset_name))
                for thename in inventory.datasets.iterkeys():
                    if pattern.match(thename):
                        data_names.append(thename)
            elif dset_name in inventory.datasets:
                    data_names.append(dset_name)
            if len(data_names) < 1: data_names = None


        cpquests = self.copy_manager.get_requests(sites=site_names, items=data_names)
        dequests = self.dele_manager.get_requests(sites=site_names, items=data_names)

        a1 = self.pro_requests(cpquests,request,inventory)
        a2 = self.pro_requests(dequests,request,inventory)
        response = a1 + a2

        return {'request': response}
                
# exported to __init__.py
export_data = {
    'requestlist': RequestList
}
