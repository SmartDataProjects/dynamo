import logging

from dynamo.utils.interface.mysql import MySQL
from base import BaseHandler

logger = logging.getLogger(__name__)

class UserRequest:
    def __init__(self, dataset, site, tstamp = 0, is_active = False):
        self.reqid = 0
        self.dataset = dataset
        self.site = site
        self.nrequests = 0
        self.is_active = is_active
        self.status = 'new'
        self.updated = False

        self._earliest = tstamp
        self._latest = 0

    def __hash__(self):
        return hash((self.dataset, self.site))

    def __eq__(self, other):
        if self.dataset == other.dataset and self.site == other.site:
            return True
        else:
            return False

    def updateRequest(self, tstamp, is_active = False):
        self.updated = True
        if self.nrequests == 0:
            self._earliest = tstamp
        
        if self._earliest > tstamp:
            self._earliest = tstamp
        if self._latest < tstamp:
            self._latest = tstamp

        self.nrequests += 1
        if not self._is_active and is_active == True:
            self._is_active = True

    def request_time(self):
        return self._earliest

        
class DirectRequestsHandler(BaseHandler):
    """
    Create dataset transfer proposals from direct user requests.
    """

    def __init__(self):
        BaseHandler.__init__(self, 'Direct')
        self._mysql = MySQL(**config.registry.db_params)

    def release_requests(self, table, reqs2delete):
        array = []
        for did, site in reqs2delete:
            array.append((did, site, 'copy'))

        self._mysql.delete_many(table, ('item', 'site', 'reqtype'), array)

    def get_requests(self, inventory, policy): # override
        self._mysql.query("LOCK TABLES `requests` WRITE")

        try:
            self._get_requests(inventory, policy)
        finally:
            self._mysql.query("UNLOCK TABLES")

    def _get_requests(self, inventory, policy):
        requests = {}
        newRequests = {}

        reqs2delete = []
        unif2delete = []

        reqs = self._mysql.query("SELECT `reqid`, `item`, `site`, `rank`, `status`, `created` FROM `requests_unified`")
        for reqid, dset, target, rank, status, create_datetime in reqs:
            reqtime = int(time.mktime(create_datetime.timetuple()))

            request = UserRequest(dset, target, reqtime, True)
            request.reqid = reqid
            request.nrequests = rank
            request.status = status

            requests[(dset, target)] = request

            reps = inventory.datasets[dset].replicas
            fullreps = [i for i in reps if i.is_complete()]

            if len([i for i in fullreps if i.site.name == target]) != 0:
                logger.debug(dset)
                logger.debug(" request already done, trash it")
                unif2delete.append((dset, target))

        reqs = self._mysql.query("SELECT `item`, `site`, `reqtype`, `created` FROM `requests`")
        for dset, target, reqtype, create_datetime in reqs:
            if create_datetime != None:
                reqtime = int(time.mktime(create_datetime.timetuple()))
            else:
                reqtime = int(time.time())
                      
            #we only deal with copy requests here
            if reqtype != 'copy':
                logger.debug(dset)
                logger.debug(" ignoring non-copy request")
                continue

            #pass only reqyests with data known to inverntory
            if dset not in inventory.datasets:
                logger.debug(dset)
                logger.debug(" non existing dataset, trash it ")
                reqs2delete.append((dset, target))
                continue

            #check that the full replicas exist anywhere
            reps = inventory.datasets[dset].replicas
            fullreps = [i for i in reps if i.is_complete()]
            if len(fullreps) < 1:
                logger.debug(dset)
                logger.debug(" no full replicas exist, ingnoring")
                continue
            
            #check if this dataset already exists in full at target site
            if len([i for i in fullreps if i.site.name == target]) != 0:
                logger.debug(dset)
                logger.debug(" request already done, trash it")
                reqs2delete.append((dset, target))
                continue
                
            if (dset, target) not in requests:
                requests[(dset, target)] = UserRequest(dset, target)

            requests[(dset, target)].updateRequest(reqtime, False)

        for (dset, target), request in requests.items():
            #if is_active true it means we already acting upon it
            #collapse all other requests and update the date
            if request.is_active:
                logger.debug(dset)
                logger.debug("master request is in")
                reqs2delete.append((dset, target))

        for (dset, target), request in requests.items():
            created = datetime.datetime.fromtimestamp(request.request_time())
            if not request.is_active:
                newRequests[(dset, target)] = request

            elif request.updated:
                logger.debug("old request rank = " + str(request.nrequests))

                sql = "UPDATE `requests_unified` SET `rank` = %d" % request.nrequests
                sql += " WHERE `reqid` = %d" % request.reqid
                self._mysql.query(sql)

        self.release_requests('requests', reqs2delete)
        self.release_requests('requests_unified', unif2delete)
        self.release_lock()

        for (dset, target), request in requests.items():
            if request.is_active and request.status == 'new':
                newRequests[(dset, target)] = request

        datasets_to_request = []

        logger.debug("\n attaching copy requests for datasets:")
        for req in sorted(newRequests.values(), key = lambda x: x.nrequests, reverse = True):
            ds = inventory.datasets[req.dataset]
            logger.debug(ds.name)
            datasets_to_request.append((ds, inventory.sites[req.site]))

        return datasets_to_request
