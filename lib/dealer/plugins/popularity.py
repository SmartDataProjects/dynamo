import time, datetime
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
        BaseHandler.__init__(self, 'Popularity')
        self.used_demand_plugins.append('dataset_request')

        self._datasets = []

    def get_requests(self, inventory, policy): # override
        self._datasets = []
        requests = []

        for dataset in inventory.datasets.values():
            if dataset.replicas is None:
                # this dataset has no replica in the pool to begin with
                continue

            try:
                request_weight = dataset.demand['request_weight']
            except KeyError:
                continue

            if request_weight <= 0.:
                continue

            if dataset.size * 1.e-12 > config.max_dataset_size:
                continue

            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if policy.partition(block_replica):
                        break
                else:
                    # no block replica in partition
                    continue

                # this replica is (partially) in partition
                break

            else: # no block in partition
                continue

            self._datasets.append(dataset)

            num_requests = min(config.max_replicas, int(math.ceil(request_weight / config.request_to_replica_threshold))) - len(dataset.replicas)
            if num_requests <= 0:
                continue

            requests.append((dataset, num_requests))
            
        requests.sort(key = lambda x: x[0].demand['request_weight'], reverse = True)

        datasets_to_request = []

        # [(d1, n1), (d2, n2), ...] -> [d1, d2, .., d1, ..] (d1 repeats n1 times)
        while True:
            added_request = False
            for ir in xrange(len(requests)):
                dataset, num_requests = requests[ir]
                if num_requests == 0:
                    continue

                datasets_to_request.append(dataset)
                requests[ir] = (dataset, num_requests - 1)
                added_request = True

            if not added_request:
                break
        
        return datasets_to_request

    def save_record(self, run_number, history, copy_list): # override
        history.save_dataset_popularity(run_number, self._datasets)


class UserRequest:
    def __init__(self,did,site,tstamp=0,inaction=False):
        self._reqid = 0
        self._id = did
        self._site = site
        self._nrequests = 0
        self._earliest = tstamp
        self._latest = 0
        self._status = 'new'
        self._updated = False
        self._inaction = False
        if inaction == True:
            self._inaction = True

    def __hash__(self):
        return hash((self._id, self._site))
    def __eq__(self, other):
        if self._id == other._id:
            if self._site == other._site:
                return True
        return False

    def updateRequest(self,tstamp,inaction=False):
        self._updated=True
        if self._nrequests == 0:
            self._earliest = tstamp
        
        if self._earliest > tstamp:
            self._earliest = tstamp
        if self._latest < tstamp:
            self._latest = tstamp

        self._nrequests += 1
        if self._inaction == False:
            if inaction == True:
                self._inaction = True
    def site(self):      return self._site
    def popularity(self):return self._nrequests
    def requested(self): return self._earliest
    def rank(self):      return self._nrequests
    def isactive(self):  return self._inaction
    def reqId(self):     return self._reqid
    def status(self):    return self._status
    def updated(self):   return self._updated
        
class DirectRequestsHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self,'LastUsed')
        self._reqtable = 'requests'
        self._unified = 'requests_unified'
        self._datasets = []
        self._requests = []
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

    def read_history(self):
        return self._mysqlHist.query("select * from " + self._reqhistory)

    def release_requests(self,table,reqs2delete):
        array = []
        for (did,site) in reqs2delete:
                array.append((did,site,'copy'))
        self._mysql.delete_many(table, ('item','site','reqtype'), array)

    def update_unified(self,idname):
        for (dset,target) in  self._requests:
            request = self._requests[(dset,target)]
            created = datetime.datetime.fromtimestamp(request.requested())
            if not request.isactive():
                #arrayNew.append((dset,'dataset',target,'copy',created,created))
                self._newRequests[(dset,target)] = request
            else:
                if request.updated():
                    print "old request rank = " + str(request.rank())
                    sql = "update requests_unified set rank=" + str(request.rank())
                    sql = sql + " where reqid=" + str(request.reqId())  
                    self._mysql.query(sql)

    def get_requests(self, inventory, policy): # override
        nowtime = int(time.time())

        self._requests = {}
        self._newRequests = {}
        self._datasets = []

        reqs2delete = {}
        unif2delete = {}
        self.get_requests_lock()

        reqs = self._mysql.query("select * from " + self._unified)
        for item in reqs:
            dset = item[1]
            target = item[3]
            reqtime = int(time.mktime(item[7].timetuple()))
            self._requests[(dset,target)] = UserRequest(dset,target,reqtime,True)
            self._requests[(dset,target)]._reqid = int(item[0])
            self._requests[(dset,target)]._nrequests = int(item[5])
            self._requests[(dset,target)]._status = item[6]
            reps = inventory.datasets[dset].replicas
            fullreps = [i for i in reps if i.is_complete==True]
            if len([i for i in fullreps if i.site.name==target])>0:
                print dset
                print " request already done, trash it"
                unif2delete[(dset,target)] = True

        reqs = self._mysql.query("select * from " + self._reqtable)
        for item in reqs:
            dset = item[0]
            target = item[2]
            reqtype = item[3]
            if item[4] != None:
                reqtime = int(time.mktime(item[4].timetuple()))
            else:
                reqtime = time.time()
                      
            #we only deal with copy requests here
            if reqtype != 'copy':
                print dset
                print " ignoring non-copy request"
                continue
            #pass only reqyests with data known to inverntory
            if dset not in inventory.datasets:
                print dset
                print " non existing dataset, trash it "
                reqs2delete[(dset,target)] = True
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
                reqs2delete[(dset,target)] = True
                continue
                
            if (dset,target) not in self._requests:
                self._requests[(dset,target)] = UserRequest(dset,target)
            self._requests[(dset,target)].updateRequest(reqtime,False)

        for (dset,target) in  self._requests:
            #if inaction true it means we already acting upon it
            #collapse all other requests and update the date
            if self._requests[(dset,target)].isactive():
                print dset
                print "master request is in"
                reqs2delete[(dset,target)] = True
                
        self.update_unified('item')
        self.release_requests('requests',reqs2delete)
        self.release_requests('requests_unified',unif2delete)
        self.release_lock()

        for (dset,target) in  self._requests:
            request = self._requests[(dset,target)]
            if request.isactive() and request.status() == 'new':
                self._newRequests[(dset,target)] = request

        print "\n attaching copy requests for datasets:"
        for req in sorted(self._newRequests.values(), key=lambda x: x.rank(), reverse=True):
            ds = inventory.datasets[req._id]
            print ds.name
            self._datasets.append((ds,inventory.sites[req._site]))

        return list(self._datasets)

plugins['Popularity'] = PopularityHandler()
plugins['DirectRequests'] = DirectRequestsHandler()

if __name__ == '__main__':
    pass
