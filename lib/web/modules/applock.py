import time

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.exceptions import MissingParameter, ExtraParameter
from dynamo.registry.registry import RegistryDatabase

class ApplockBase(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.registry = RegistryDatabase()

    def _validate_request(self, request, required, allowed = None):
        for key in required:
            if key not in request:
                raise MissingParameter(key)

        for key in request.iterkeys():
            if key not in required and key not in allowed:
                raise ExtraParameter(key)


class ApplockCheck(ApplockBase):
    """
    Check the lock status of an application.
    """

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'])
        user, service, timestamp, note, depth = self.registry.get_app_lock(request['app'])

        if user is None:
            self.message = 'Not locked'
            return None
        else:
            self.message = 'Locked'
            data = {'user': user, 'service': service, 'lock_time': timestamp, 'depth': depth}
            if note is not None:
                data['note'] = note

            return data


class ApplockLock(ApplockBase):
    """
    Lock an application.
    """
    def __init__(self, config):
        ApplockBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'], ['service', 'note'])

        if 'service' in request:
            request_service = request['service']
        else:
            request_service = None

        if 'note' in request:
            note = request['note']
        else:
            note = None

        self.registry.lock_app(request['app'], caller.name, request_service, note)

        user, service, timestamp, note, depth = self.registry.get_app_lock(request['app'])

        if user is None:
            # cannot happen but for safety
            self.message = 'Not locked'
            return None
        else:
            if user == caller.name and service == request_service:
                self.message = 'Success'
            else:
                self.message = 'Wait'

            data = {'user': user, 'service': service, 'lock_time': timestamp, 'depth': depth}
            if note is not None:
                data['note'] = note

            return data


class ApplockUnlock(ApplockBase):
    """
    Unlock an application.
    """
    def __init__(self, config):
        ApplockBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'], ['service'])

        if 'service' in request:
            service = request['service']
        else:
            service = None

        self.registry.unlock_app(request['app'], caller.name, service)

        user, service, timestamp, note, depth = self.registry.get_app_lock(request['app'])

        if user is None:
            self.message = 'Unlocked'
            return None
        else:
            self.message = 'Locked'
            data = {'user': user, 'service': service, 'lock_time': timestamp, 'depth': depth}
            if note is not None:
                data['note'] = note

            return data


class CurrentApps(WebModule):
    """
    List currently running applications. Not quite an app "lock".
    """
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.require_appmanager = True

    def run(self, caller, request, inventory):
        result = []
        for title, write_request, host, queued_time in self.appmanager.get_running_processes():
            result.append({'title': title, 'write_request': write_request, 'host': host, 'queued_time': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(queued_time))})

        return result


class ApplockHelp(WebModule, HTMLMixin):
    """
    Show a help webpage
    """

    def __init__(self, config):
        WebModule.__init__(self, config) 
        HTMLMixin.__init__(self, 'Dynamo application locks API', 'applock/help.html')

    def run(self, caller, request, inventory):
        return self.form_html({})

    
export_data = {
    'check': ApplockCheck,
    'lock': ApplockLock,
    'unlock': ApplockUnlock,
    'current': CurrentApps
}

export_web = {
    'help': ApplockHelp
}

# backward compatibility
registry_alias = 'applock'
