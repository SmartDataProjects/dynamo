import time
import logging

from dynamo.web.exceptions import InvalidRequest, MissingParameter
from dynamo.web.modules._base import WebModule
from dynamo.web.modules.request.mixin import ParseInputMixin
from dynamo.request.copy import CopyRequestManager
import dynamo.dataformat as df
from dynamo.dataformat.request import Request

LOG = logging.getLogger(__name__)

class CopyRequestBase(WebModule, ParseInputMixin):
    """
    Base class for copy requests. Initialize with an input parser.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        ParseInputMixin.__init__(self, config)

        self.manager = CopyRequestManager()


class MakeCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)
        self.must_authenticate = True

        # config.request.copy points to the "copy" method of dict
        self.default_group = config['request']['copy']['default_group']
        self.default_sites = config['request']['copy'].get('default_sites', [])

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'group', 'n', 'cache'))

        self.manager.lock()
        LOG.info("aV1: %s" % str(time.time()))

        try:
            existing = None

            if 'request_id' in self.params:
                request_id = self.params['request_id']

                constraints = self.make_constraints(by_id = True)
                existing_requests = self.manager.get_requests(**constraints)

                if len(existing_requests) == 0:
                    raise InvalidRequest('Invalid request id %d' % request_id)

                existing = existing_requests[request_id]

                if existing.status != Request.ST_NEW:
                    raise InvalidRequest('Request %d cannot be updated any more' % request_id)

            else:
                LOG.info("aV2: %s" % str(time.time()))

                # create a new request
                if 'item' not in self.params:
                    raise MissingParameter('item')

                if 'site' not in self.params:
                    if len(self.default_sites) == 0:
                        raise MissingParameter('site')
                    else:
                        self.params['site'] = list(self.default_sites)

                if 'cache' not in self.params:
                    # This only has to be done if we do not want to stupidly dump things into the cache table
                    constraints = self.make_constraints(by_id = False)

                    constraints['statuses'] = [Request.ST_NEW, Request.ST_ACTIVATED]
                    existing_requests = self.manager.get_requests(**constraints)

                    for request_id in sorted(existing_requests.iterkeys()):
                        if existing_requests[request_id].status == Request.ST_NEW:
                            existing = existing_requests[request_id]
                            break
                        elif existing_requests[request_id].status == Request.ST_ACTIVATED:
                            existing = existing_requests[request_id]

            if existing is None:
                if 'n' not in self.params:
                    self.params['n'] = 1
        
                if 'group' not in self.params:
                    self.params['group'] = self.default_group
                    
                if 'cache' not in self.params:
                    LOG.info("Create request")
                    LOG.info("aV3: %s" % str(time.time()))
                    request = self.manager.create_request(caller, self.params['item'], self.params['site'], self.params['site_orig'], self.params['group'], self.params['n'])
                else:
                    # We want to allow the requester to just place the request info in a cache table that dynamo will act on by itself
                    LOG.info("Creating caching request")
                    LOG.info("aV4: %s" % str(time.time()))
                    request = self.manager.create_cached_request(caller, self.params['item'][0], " ".join(self.params['site_orig']), self.params['group'], self.params['n'])
            else:
                existing.request_count += 1
                existing.last_request = int(time.time())

                if existing.status == Request.ST_NEW:
                    # allow update of values
                    if 'group' in self.params:
                        existing.group = self.params['group']
                    if 'n' in self.params:
                        existing.n = self.params['n']

                self.manager.update_request(existing)

                request = existing

        finally:
            try:
                self.manager.unlock()
            except:
                LOG.error('Error in manager.unlock()')

        # requests is a single-element dictionary
        if 'cache' in self.params:
            return [request]
        else:
            return [request.to_dict()] 


class PollCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))

        constraints = self.make_constraints(by_id = False)

        LOG.info("PollCopy constraints:")
        LOG.info(constraints)

        existing_requests = self.manager.get_requests(**constraints)

        if 'item' in self.params and 'site' in self.params and \
                ('all' not in self.params or not self.params['all']):
            # this was a query by item and site - if show-all is not requested, default to showing the latest
            max_id = max(existing_requests.iterkeys())
            existing_requests = {max_id: existing_requests[max_id]}

        if len(existing_requests) != 0:
            self.message = 'Request found'
        else:
            self.message = 'Request not found'

        return [r.to_dict() for r in existing_requests.itervalues()]


class CancelCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id',), ('request_id',))

        request_id = self.params['request_id']

        self.manager.lock()
        
        try:
            constraints = self.make_constraints(by_id = True)
            existing_requests = self.manager.get_requests(**constraints)

            if len(existing_requests) == 0:
                raise InvalidRequest('Invalid request id %d' % request_id)
                
            existing = existing_requests[request_id]

            if existing.status == Request.ST_NEW:
                existing.status = Request.ST_CANCELLED
                self.update_request(existing)

            elif existing.status == Request.ST_CANCELLED:
                pass

            else:
                raise InvalidRequest('Request %d cannot be cancelled any more' % request_id)

        finally:
            self.manager.unlock()

        return existing.to_dict()


export_data = {
    'copy': MakeCopyRequest,
    'pollcopy': PollCopyRequest,
    'cancelcopy': CancelCopyRequest
}
