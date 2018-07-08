import time

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest
from dynamo.registry.registry import RegistryDatabase

class ApplockBase(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.require_authorizer = True
        self.registry = RegistryDatabase()

    def _validate_request(self, request, required, allowed = None):
        for key in required:
            if key not in request:
                raise MissingParameter(key)

        for key in request.iterkeys():
            if key not in required and key not in allowed:
                raise ExtraParameter(key)

    def _get_lock(self, app):
        # this function can be called within a table lock, so we need to lock what we use
        self.registry.db.lock_tables(read = [('activity_lock', 'l'), ('user_services', 's')])

        sql = 'SELECT l.`user_id`, s.`name`, UNIX_TIMESTAMP(l.`timestamp`), l.`note` FROM `activity_lock` AS l'
        sql += ' LEFT JOIN `user_services` AS s ON s.`id` = l.`service_id`'
        sql += ' WHERE l.`application` = %s ORDER BY l.`timestamp` ASC';

        lock_data = self.registry.db.query(sql, app)

        self.registry.db.unlock_tables()

        if len(lock_data) == 0:
            return None, None, None, None, 0

        il = 0
        while il != len(lock_data):
            first_uid, first_service, lock_time, note = lock_data[il]
            user_info = self.authorizer.identify_user(uid = first_uid)
            il += 1

            if user_info is None:
                # this can accumulate - need to delete some time
                continue
            else:
                break

        if user_info is None:
            return None, None, None, None, 0

        depth = 1
        
        for uid, service, _, _ in lock_data[il:]:
            if uid == first_uid and service == first_service:
                depth += 1
                
        return user_info[0], first_service, lock_time, note, depth

class ApplockCheck(ApplockBase):
    """
    Check the lock status of an application.
    """

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'])
        user, service, timestamp, note, depth = self._get_lock(request['app'])

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

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'], ['service', 'note'])

        request_service = None
        service_id = 0
        if 'service' in request:
            try:
                sql = 'SELECT `id` FROM `user_services` WHERE `name` = %s'
                service_id = self.registry.db.query(sql, request['service'])[0]
                request_service = request['service']
            except IndexError:
                pass

        if 'note' in request:
            note = request['note']
        else:
            note = None
    
        sql = 'INSERT INTO `activity_lock` (`user_id`, `service_id`, `application`, `timestamp`, `note`)'
        sql += ' VALUES (%s, %s, %s, NOW(), %s)'

        self.registry.db.query(sql, caller.id, service_id, request['app'], note)

        user, service, timestamp, note, depth = self._get_lock(request['app'])

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

    def run(self, caller, request, inventory):
        self._validate_request(request, ['app'], ['service'])

        service_id = 0
        if 'service' in request:
            try:
                sql = 'SELECT `id` FROM `user_services` WHERE `name` = %s'
                service_id = self.registry.db.query(sql, request['service'])[0]
            except IndexError:
                pass

        self.registry.db.lock_tables(write = ['activity_lock', ('activity_lock', 'l')])

        sql = 'DELETE FROM `activity_lock` WHERE `id` = ('
        sql += ' SELECT m FROM ('
        sql += '  SELECT MAX(`id`) m FROM `activity_lock` AS l'
        sql += '  WHERE `user_id` = %s AND `service_id` = %s AND `application` = %s'
        sql += ' ) AS tmp'
        sql += ')'
        self.registry.db.query(sql, caller.id, service_id, request['app'])

        user, service, timestamp, note, depth = self._get_lock(request['app'])

        # a little cleanup
        if self.registry.db.query('SELECT COUNT(*) FROM `activity_lock`')[0] == 0:
            self.registry.db.query('ALTER TABLE `activity_lock` AUTO_INCREMENT = 1')

        self.registry.db.unlock_tables()

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
