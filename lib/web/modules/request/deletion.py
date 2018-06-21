import time
import calendar
import json
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._userdata import UserDataMixin
from dynamo.web.modules.request.mixin import ParseInputMixin
from dynamo.request.deletion import DeletionRequestManager
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class DeletionRequestBase(WebModule, UserDataMixin, ParseInputMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        UserDataMixin.__init__(self, config)
        ParseInputMixin.__init__(self, config)

        manager_config = df.Configuration(registry = config.registry, history = config.history)

        self.manager = DeletionRequestManager(manager_config)


class MakeDeletionRequest(DeletionRequestBase):
    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('item', 'site'), ('item', 'site'))

        self.manager.lock()

        try:
            constraints = self.make_constraints(by_id = False)
            existing_requests = self.manager.get_requests(self.authorizer, **constraints)

            existing = None

            for request_id in sorted(existing_requests.iterkeys()):
                if existing_requests[request_id].status == 'new':
                    existing = existing_requests[request_id]
                    break
                elif existing_requests[request_id].status == 'activated':
                    existing = existing_requests[request_id]

            if existing is not None:
                return [existing.to_dict()]

            else:
                request = self.manager.create_request(caller, self.params['item'], self.params['site'])
                return [request.to_dict()]

        finally:
            self.manager.unlock()


class PollDeletionRequest(DeletionRequestBase):
    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))
    
        constraints = self.make_constraints(by_id = False)
        existing_requests = self.manager.get_requests(self.authorizer, **constraints)

        return [r.to_dict() for r in existing_requests.itervalues()]


export_data = {
    'delete': MakeDeletionRequest,
    'polldelete': PollDeletionRequest
}
