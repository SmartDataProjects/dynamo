import logging

from dynamo.dealer.plugins.base import BaseHandler
from dynamo.utils.interface.mysql import MySQL

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

    def update_request(self, tstamp, set_active = False):
        self.updated = True
        if self.nrequests == 0:
            self._earliest = tstamp
        
        if self._earliest > tstamp:
            self._earliest = tstamp
        if self._latest < tstamp:
            self._latest = tstamp

        self.nrequests += 1
        if set_active:
            self._is_active = True

    def request_time(self):
        return self._earliest

        
class DirectRequestsHandler(BaseHandler):
    """
    Create dataset transfer proposals from direct user requests.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Direct')

        self.registry = MySQL(config.db_config)

    def release_requests(self, table, reqs2delete):
        array = []
        for did, site in reqs2delete:
            array.append((did, site, 'copy'))

        self._mysql.delete_many(table, ('item', 'site', 'reqtype'), array)

    def get_requests(self, inventory, policy): # override
        requests = {}
        newRequests = {}

        reqs2delete = []
        unif2delete = []

        reqs = self._mysql.xquery("SELECT `reqid`, `item`, `site`, `rank`, `status`, `created` FROM `requests_unified`")
        for reqid, item, target, rank, status, create_datetime in reqs:
            try:
                dataset = inventory.datasets[item]
            except KeyError:
                logger.debug("Dataset %s not known to inventory. Request rejected" % item)
                unif2delete.append((item, target))
                continue

            reqtime = int(time.mktime(create_datetime.timetuple()))

            request = UserRequest(item, target, reqtime, True)
            request.reqid = reqid
            request.nrequests = rank
            request.status = status

            requests[(item, target)] = request

            if dataset.replicas is None:
                inventory.store.load_replicas(dataset)

            try:
                next(rep for rep in dataset.replicas if rep.is_complete() and rep.site.name == target)
            except StopIteration:
                pass
            else:
                logger.debug(item)
                logger.debug(" request already done, trash it")
                unif2delete.append((item, target))

        reqs = self._mysql.xquery("SELECT `item`, `site`, `reqtype`, `created` FROM `requests` WHERE `reqtype` = 'copy'")
        for item, target, reqtype, create_datetime in reqs:
            if create_datetime != None:
                reqtime = int(time.mktime(create_datetime.timetuple()))
            else:
                reqtime = int(time.time())
                      
            #pass only requests with data known to inverntory
            try:
                dataset = inventory.datasets[item]
            except KeyError:
                logger.debug(item)
                logger.debug(" non existing dataset, trash it ")
                reqs2delete.append((item, target))
                continue

            if dataset.replicas is None:
                inventory.store.load_replicas(dataset)

            #check that the full replicas exist anywhere
            fullreps = filter(lambda rep: rep.is_complete(), dataset.replicas)
            if len(fullreps) == 0:
                logger.debug(item)
                logger.debug(" no full replicas exist, ignoring")
                continue
            
            #check if this dataset already exists in full at target site
            try:
                next(rep for rep in fullreps if rep.site.name == target)
            except StopIteration:
                pass
            else:
                logger.debug(item)
                logger.debug(" request already done, trash it")
                reqs2delete.append((item, target))
                continue
                
            if (item, target) not in requests:
                requests[(item, target)] = UserRequest(item, target)

            requests[(item, target)].update_request(reqtime, False)

        for request in requests.itervalues():
            #if is_active true it means we already acting upon it
            #collapse all other requests and update the date
            if request.is_active:
                logger.debug(request.dataset)
                logger.debug("master request is in")
                reqs2delete.append((request.dataset, request.site))

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
