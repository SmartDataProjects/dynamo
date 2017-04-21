import time
import math
import logging

from common.interface.mysql import MySQL
import common.configuration as comm_config
from dealer.plugins._list import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config


logger = logging.getLogger(__name__)

class PopularityHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)
        self._datasets = []

    def get_requests(self, inventory, partition): # override
        self._datasets = []

        for dataset in inventory.datasets.values():
            if dataset.demand.request_weight <= 0.:
                continue

            if dataset.size() * 1.e-12 > config.max_dataset_size:
                continue

            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if partition(block_replica):
                        break
                else:
                    # no block replica in partition
                    continue

                # this replica is (partially) in partition
                break

            else: # no block in partition
                continue

            num_requests = min(config.max_replicas, int(math.ceil(dataset.demand.request_weight / config.request_to_replica_threshold))) - len(dataset.replicas)
            for _ in xrange(num_requests):
                self._datasets.append(dataset)
            
        self._datasets.sort(key = lambda dataset: dataset.demand.request_weight, reverse = True)
        
        return list(self._datasets), [], []

    def save_record(self, run_number, history, copy_list): # override
        history.save_dataset_popularity(run_number, self._datasets)


class UserRequest:
    def __init__(self,did,site):
        self._id = did
        self._site = site
        self._nrequests = 0
        self._earliest = 0
        self._latest = 0
        self._rank = 0
    def __hash__(self):
        return hash((self._id, self._site))
    def __eq__(self, other):
        if self._id == other._id:
            if self._site == other._site:
                return True
        return False

    def addRequestTime(self,tstamp,nowtime):
        if self._nrequests == 0:
            self._earliest = tstamp
        
        if self._earliest > tstamp:
            self._earliest = tstamp
        if self._latest < tstamp:
            self._latest = tstamp
        self._nrequests += 1
        self._rank += (nowtime - tstamp)  
    def site(self):
        return self._site
    def popularity(self):
        return self._nrequests
    def requested(self):
        return self._earliest
    def rank(self):
        return self._rank
        
class DirectRequestsHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)
        self._reqtable = 'requests'
        self._datasets = []
        self._requests = []
        self._deleteRequests = {}
        self._mysql = MySQL(**comm_config.mysqlregistry.db_params)

    def get_requests_lock(self):
        while(True):
            islocked = 0
            resplines = self._mysql.query("SHOW OPEN TABLES LIKE '" + self._reqtable + "'")
            for item in resplines:
                dbName = item[0]
                if dbName != self._mysql.db_name():
                    continue
                else:
                    islocked = item[2]
        
            if not islocked:
                break
        self._mysql.query("LOCK TABLES " + self._reqtable + " WRITE")

    def release_lock(self):
        self._mysql.query("UNLOCK TABLES")

    def read_requests(self):
        return self._mysql.query("select * from " + self._reqtable)

    def release_requests(self,idname):
        array = []
        for did in self._deleteRequests:
            for site in self._deleteRequests[did]:
                array.append((did,site,'copy'))
        self._mysql.delete_many('requests', (idname,'site','reqtype'), array)

    def get_requests(self, inventory, partition): # override
        nowtime = int(time.time())

        self._deleteRequests = {}
        self._requests = {}
        self._datasets = []

        self.get_requests_lock()
        reqs = self.read_requests()
        for item in reqs:
            dset = item[0]
            target = item[2]
            reqtype = item[3]
            reqtime = int(time.mktime(item[4].timetuple()))

            #we only deal with copy requests here
            if reqtype != 'copy':
                print dset
                print " ignoring non-copy request"
                continue
            #pass only reqyests with data known to inverntory
            if dset not in inventory.datasets:
                print dset
                print " non existing dataset, trash it "
                if dset not in self._deleteRequests:                                              
                    self._deleteRequests[dset] = []
                self._deleteRequests[dset].append(target)
                continue

            #check that the full replicas exist anywhere
            reps = inventory.datasets[dset].replicas
            fullreps = [i for i in reps if i.is_complete==True]
            if len(fullreps) < 1:
                print dset
                print " no full replicas exist, ingnoring"
                continue
            
            #check if this dataset already exists in full at target site
            if len([i for i in fullreps if i.site.name==target])>0:
                print dset
                print " request already done, trash it"
                if dset not in self._deleteRequests:
                    self._deleteRequests[dset] = []
                self._deleteRequests[dset].append(target)
                continue
                
            if (dset,target) not in self._requests:
                self._requests[(dset,target)] = UserRequest(dset,target)
            self._requests[(dset,target)].addRequestTime(reqtime,nowtime)

        self.release_requests('item')
        self.release_lock()

        print "\n attaching copy requests for datasets:"
        for req in sorted(self._requests.values(), key=lambda x: x._rank, reverse=True):
            ds = inventory.datasets[req._id]
            print ds.name
            self._datasets.append((ds,inventory.sites[req._site]))

        return list(self._datasets), [], []

plugins['Popularity'] = PopularityHandler()
plugins['DirectRequests'] = DirectRequestsHandler()

if __name__ == '__main__':
    pass
