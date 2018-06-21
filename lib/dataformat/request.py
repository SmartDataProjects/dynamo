"""
Classes representing copy and deletion requests. Not linked to the inventory.
"""

from block import Block
from exception import ObjectError

class Request(object):
    """
    Base class for copy and deletion requests.
    """

    __slots__ = ['request_id', 'user', 'user_dn', 'status', 'reject_reason', 'sites', 'items', 'actions']

    ST_NEW, ST_ACTIVATED, ST_COMPLETED, ST_REJECTED, ST_CANCELLED = range(1, 6)
    status_names = ['', 'new', 'activated', 'completed', 'rejected', 'cancelled']

    def __init__(self, request_id, user, user_dn, status, reject_reason = None):
        self.request_id = request_id
        self.user = user
        self.user_dn = user_dn
        if type(status) is str:
            self.status = eval('Request.ST_' + status.upper())
        else:
            self.status = status
        self.reject_reason = reject_reason
        self.sites = []
        self.items = []
        self.actions = None

    def to_dict(self):
        d = {
            'request_id': self.request_id,
            'item': self.items,
            'site': self.sites,
            'status': Request.status_names[self.status],
            'user': self.user,
            'dn': self.user_dn
        }

        if self.status == Request.ST_REJECTED:
            d['reason'] = self.reject_reason

        elif self.status in (Request.ST_ACTIVATED, Request.ST_COMPLETED):
            actions = d['active'] = []
            # active_copies must be non-null
            for a in self.actions:
                actions.append({
                    'item': a.item,
                    'site': a.site,
                    'status': RequestAction.status_names[a.status],
                    'updated': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(a.last_update))
                })
        
        return d

    def find_items(self, inventory, logger = None):
        """
        Find datasets and blocks in the items list from the inventory.
        @param inventory   DynamoInventory object

        @return  {dataset: set of blocks or None}, None if the item is a dataset.
        """

        datasets = {}
    
        for item in self.items:
            try:
                dataset = inventory.datasets[item]
            except KeyError:
                try:
                    dataset_name, block_name = Block.from_full_name(item)
                except ObjectError:
                    if logger:
                        logger.error('Invalid item name %s', item)
                else:
                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        if logger:
                            logger.error('Unknown dataset %s', dataset_name)
                    else:
                        if dataset in datasets and datasets[dataset] is None:
                            continue
    
                        block = dataset.find_block(block_name)
                        if block is None:
                            if logger:
                                logger.error('Unknown block %s', Block.to_real_name(block_name))
                        else:
                            if dataset in datasets:
                                datasets[dataset].add(block)
                            else:
                                datasets[dataset] = set([block])
    
            else:
                datasets[dataset] = None

        return datasets

    def find_sites(self, inventory, logger = None):
        """
        Find sites in the sites list from the inventory.
        @param inventory   DynamoInventory object

        @return  List of sites.
        """

        sites = []
        for site_name in self.sites:
            try:
                site = inventory.sites[site_name]
                sites.append(site)
            except KeyError:
                # requests should be validated, so this should not happen
                if logger:
                    logger.error('Unknown site %s', site_name)

        return sites

    def activate(self, activation_list):
        self.actions = []

        for item, site, timestamp in activation_list:
            self.actions.append(RequestAction(item, site, RequestAction.ST_NEW, timestamp))

        self.status = Request.ST_ACTIVATED


class RequestAction(object):
    __slots__ = ['item', 'site', 'status', 'last_update']

    ST_NEW, ST_QUEUED, ST_FAILED, ST_COMPLETED = range(1, 5)
    status_names = ['', 'new', 'queued', 'failed', 'completed']

    def __init__(self, item, site, status, last_update):
        self.item = item
        self.site = site
        if type(status) is str:
            self.status = eval('RequestAction.ST_' + status.upper())
        else:
            self.status = status
        self.last_update = update
    

class CopyRequest(Request):
    """
    Utility class to carry around all relevant information about a request.
    """

    __slots__ = ['group', 'n', 'first_request', 'last_request', 'request_count']

    def __init__(self, request_id, user, user_dn, group, n, status, first_request, last_request, request_count, reject_reason = None):
        Request.__init__(self, request_id, user, user_dn, status, reject_reason)

        self.group = group
        self.n = n
        self.first_request = first_request
        self.last_request = last_request
        self.request_count = request_count

    def to_dict(self):
        d = Request.to_dict(self)
        d['group'] = self.group
        d['n'] = self.n
        d['first_request'] = time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.first_request))
        d['last_request'] = time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.last_request))
        d['request_count'] = self.request_count
        
        return d

class DeletionRequest(Request):
    """
    Utility class to carry around all relevant information about a request.
    """

    __slots__ = ['request_time']

    def __init__(self, request_id, user, user_dn, status, request_time, reject_reason = None):
        Request.__init__(self, request_id, user, user_dn, status, reject_reason)

        self.request_time = request_time

    def to_dict(self):
        d = Request.to_dict(self)
        d['request_time'] = time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.request_time))
        
        return d

