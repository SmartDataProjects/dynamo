from dynamo.web.modules._base import WebModule
from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest
from dynamo.registry.registry import RegistryDatabase

class DetoxLockBase(WebModule):
    def __init__(self, config):
        self.registry = RegistryDatabase()

    def _validate_request(self, request, inventory, required, allowed = None):
        for key in required:
            if key not in request:
                raise MissingParameter(key)

        for key in request.iterkeys():
            if key not in required and key not in allowed:
                raise ExtraParameter(key)

        if 'sites' in request:
            if type(request['sites']) is str:
                request['sites'] = request['sites'].strip(',')

            for site in request['sites']:
                if '*' in site or '?' in site:
                    pass
                elif site not in inventory.sites:
                    raise InvalidRequest('Unknown site %s' % site)

        if 'groups' in request:
            if type(request['groups']) is str:
                request['groups'] = request['groups'].strip(',')

            for group in request['groups']:
                if '*' in group or '?' in group:
                    pass
                elif group not in inventory.groups:
                    raise InvalidRequest('Unknown group %s' % group)

    def _get_lock(self, request, valid_only = False):
        pass

    def _create_lock(self, request):
        pass

    def _update_lock(self, existing, request):
        pass

    def _disable_lock(self, request):
        pass

    def _lock_tables(self):
        pass

    def _unlock_tables(self):
        pass


class DetoxLock(DetoxLockBase):
    def run(self, caller, request, inventory):
        self._validate_request(request, inventory, ['item', 'expires'], ['sites', 'groups', 'comment'])

        self._lock_tables()

        try:
            existing = self._get_lock(request, valid_only = True)
    
            if existing is None:
                # new lock
                new_locks = self._create_lock(request)
    
                self.message = 'Lock created'
                return new_locks
    
            else:
                updated_locks = self._update_lock(existing, request)
                
                self.message = 'Lock updated'
                return updated_locks

        finally:
            self._unlock_tables()


class DetoxUnlock(DetoxLockBase):
    def run(self, caller, request, inventory):
        self._validate_request(request, inventory, ['item'], ['sites', 'groups', 'comment'])

        self._lock_tables()

        try:
            existing = self._create_lock(request)

            if existing is None:
                self.message = 'No lock found'
                return None

            else:
                disabled_locks = self._disable_lock(request)

                self.message = 'Unlocked'
                return disabled_locks

        finally:
            self._unlock_tables()


class DetoxListLock(DetoxLockBase):
    def run(self, caller, request, inventory):
        pass
