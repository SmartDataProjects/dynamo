import logging

from dynamo.web.exceptions import InvalidRequest
from dynamo.web.modules._base import WebModule
from dynamo.web.modules._userdata import UserDataMixin
from dynamo.web.modules.request.mixin import ParseInputMixin
from dynamo.request.copy import CopyRequestManager
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class CopyRequestBase(WebModule, UserDataMixin, ParseInputMixin):
    """
    Base class for copy requests. Initialize with an input parser and a handle to the authorizer.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        UserDataMixin.__init__(self, config)
        ParseInputMixin.__init__(self, config)

        manager_config = df.Configuration(registry = config.registry, history = {'db_params': config.history})

        self.manager = CopyRequestManager(manager_config)


class MakeCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)

        # config.request.copy points to the "copy" method of dict
        self.default_group = config['request']['copy']['default_group']
        self.default_sites = config['request']['copy'].get('default_sites', [])

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'group', 'n'))

        self.manager.lock()

        try:
            existing = None

            if 'request_id' in self.params:
                request_id = self.params['request_id']

                constraints = self.make_constraints(by_id = True)
                existing_requests = self.manager.get_requests(self.authorizer, **constraints)

                if len(existing_requests) == 0:
                    raise InvalidRequest('Invalid request id %d' % request_id)

                existing = existing_requests[request_id]

                if existing.status != 'new':
                    raise InvalidRequest('Request %d cannot be updated any more' % request_id)

            else:
                constraints = self.make_constraints(by_id = False)
                existing_requests = self.manager.get_requests(self.authorizer, **constraints)

                for request_id in sorted(existing_requests.iterkeys()):
                    if existing_requests[request_id].status == 'new':
                        existing = existing_requests[request_id]
                        break
                    elif existing_requests[request_id].status == 'activated':
                        existing = existing_requests[request_id]

            if existing is None:
                # create a new request
                if 'item' not in self.params:
                    raise MissingParameter('item')
        
                if 'n' not in self.params:
                    self.params['n'] = 1
        
                if 'group' not in self.params:
                    self.params['group'] = self.default_group
        
                if 'site' not in self.params:
                    if len(self.default_sites) == 0:
                        raise MissingParameter('site')
                    else:
                        self.params['site'] = list(self.default_sites)

                request = self.manager.create_request(caller, self.params['item'], self.params['site'], self.params['group'], self.params['n'])

            else:
                existing.request_count += 1
                existing.last_request = time.time()

                if existing.status == 'new':
                    # allow update of values
                    if 'group' in self.params:
                        existing.group = self.params['group']
                    if 'n' in self.params:
                        existing.n = self.params['n']

                self.update_request(existing)

                request = existing

        finally:
            self.manager.unlock()

        # requests is a single-element dictionary
        return [request.to_dict()]


class PollCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))

        constraints = self.make_constraints(by_id = False)
        existing_requests = self.manager.get_requests(self.authorizer, **constraints)

        return [r.to_dict() for r in existing_requests.itervalues()]


class CancelCopyRequest(CopyRequestBase):
    def __init__(self, config):
        CopyRequestBase.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id',), ('request_id',))

        request_id = self.params['request_id']

        self.manager.lock()
        
        try:
            constraints = self.make_constraints(by_id = True)
            existing_requests = self.manager.get_requests(self.authorizer, **constraints)

            if len(existing_requests) == 0:
                raise InvalidRequest('Invalid request id %d' % request_id)
                
            existing = existing_requests[request_id]

            if existing.status == 'new':
                existing.status = 'cancelled'
                self.update_request(existing)

            elif existing.status == 'cancelled':
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
