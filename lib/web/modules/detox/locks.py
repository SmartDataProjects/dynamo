import time
import dateutil.parser as dateparser
import calendar
import logging

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest
from dynamo.registry.registry import RegistryDatabase
from dynamo.history.history import HistoryDatabase
from dynamo.utils.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

class DetoxLockBase(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.registry = RegistryDatabase()
        self.history = HistoryDatabase()

    def _validate_request(self, request, inventory, required, allowed = None):
        for key in required:
            if key not in request:
                raise MissingParameter(key)

        for key in request.iterkeys():
            if key not in required and key not in allowed:
                raise ExtraParameter(key)

        if 'lockid' in request:
            if type(request['lockid']) is str:
                lock_ids = request['lockid'].split(',')
            else:
                lock_ids = request['lockid']
            try:
                request['lockid'] = map(int, lock_ids)
            except ValueError:
                raise InvalidRequest('Invalid lock id %s' % request['lockid'])

        if 'sites' in request:
            if type(request['sites']) is str:
                request['sites'] = request['sites'].split(',')

            for site in request['sites']:
                if '*' in site or '?' in site:
                    pass
                elif site not in inventory.sites:
                    raise InvalidRequest('Unknown site %s' % site)

        if 'groups' in request:
            if type(request['groups']) is str:
                request['groups'] = request['groups'].split(',')

            for group in request['groups']:
                if '*' in group or '?' in group:
                    pass
                elif group not in inventory.groups:
                    raise InvalidRequest('Unknown group %s' % group)

        if 'user' in request:
            if type(request['user']) is str:
                request['user'] = request['user'].split(',')

        for key in ['expires', 'created_before', 'created_after', 'expires_before', 'expires_after']:
            if key in request:
                t = dateparser.parse(request[key])
                request[key] = calendar.timegm(t.utctimetuple())

    def _get_lock(self, request, valid_only = False):
        sql = 'SELECT l.`id`, l.`user`, l.`dn`, s.`name`, l.`item`, l.`sites`, l.`groups`,'
        sql += ' UNIX_TIMESTAMP(l.`lock_date`), UNIX_TIMESTAMP(l.`expiration_date`), l.`comment`'
        sql += ' FROM `detox_locks` AS l'
        sql += ' LEFT JOIN `user_services` AS s ON s.`id` = l.`service_id`'
        
        constraints = []
        args = []
        user_const = -1

        if 'lockid' in request:
            constraints.append('l.`id` IN %s' % MySQL.stringify_sequence(request['lockid']))

        if 'user' in request:
            user_const = len(constraints)
            constraints.append('l.`user` IN %s' % MySQL.stringify_sequence(request['user']))

        if 'service' in request:
            constraints.append('s.`name` = %s')
            args.append(request['service'])

        if 'item' in request:
            constraints.append('l.`item` = %s')
            args.append(request['item'])

        if 'sites' in request:
            constraints.append('l.`sites` IN %s' % MySQL.stringify_sequence(request['sites']))

        if 'groups' in request:
            constraints.append('l.`groups` IN %s' % MySQL.stringify_sequence(request['groups']))

        if 'created_before' in request:
            constraints.append('l.`lock_date` <= FROM_UNIXTIME(%s)')
            args.append(request['created_before'])

        if 'created_after' in request:
            constraints.append('l.`lock_date` >= FROM_UNIXTIME(%s)')
            args.append(request['created_after'])

        if 'expires_before' in request:
            constraints.append('l.`expiration_date` <= FROM_UNIXTIME(%s)')
            args.append(request['expires_before'])

        if 'expires_after' in request:
            constraints.append('l.`expiration_date` >= FROM_UNIXTIME(%s)')
            args.append(request['expires_after'])

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        existing = []

        for lock_id, user, dn, service, item, site, group, lock_date, expiration_date, comment in self.registry.db.xquery(sql, *args):
            lock = {
                'lockid': lock_id,
                'user': user,
                'dn': dn,
                'item': item,
                'locked': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(lock_date)),
                'expires': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(expiration_date))
            }
            if service is not None:
                lock['service'] = service
            if site is not None:
                lock['sites'] = site
            if group is not None:
                lock['groups'] = group
            if comment is not None:
                lock['comment'] = comment

            existing.append(lock)

        if valid_only or ('lockid' in request and len(existing) != 0):
            return existing

        sql = 'SELECT l.`id`, u.`name`, u.`dn`, s.`name`, l.`item`, l.`sites`, l.`groups`,'
        sql += ' UNIX_TIMESTAMP(l.`lock_date`), UNIX_TIMESTAMP(l.`unlock_date`), UNIX_TIMESTAMP(l.`expiration_date`), l.`comment`'
        sql += ' FROM `detox_locks` AS l'
        sql += ' LEFT JOIN `users` AS u ON u.`id` = l.`user_id`'
        sql += ' LEFT JOIN `user_services` AS s ON s.`id` = l.`service_id`'

        if len(constraints) != 0:
            if user_const != -1:
                constraints[user_const] = 'u.`name` IN %s' % MySQL.stringify_sequence(request['user'])

            sql += ' WHERE ' + ' AND '.join(constraints)
        
        for lock_id, user, dn, service, item, site, group, lock_date, unlock_date, expiration_date, comment in self.history.db.xquery(sql, *args):
            lock = {
                'lockid': lock_id,
                'user': user,
                'dn': dn,
                'item': item,
                'locked': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(lock_date)),
                'unlocked': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(unlock_date)),
                'expires': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(expiration_date))
            }
            if service is not None:
                lock['service'] = service
            if site is not None:
                lock['sites'] = site
            if group is not None:
                lock['groups'] = group
            if comment is not None:
                lock['comment'] = comment

            existing.append(lock)

        return existing

    def _create_lock(self, request, user, dn):
        service_id = 0
        if 'service' in request:
            try:
                service_id = self.registry.db.query('SELECT `id` FROM `user_services` WHERE `name` = %s', request['service'])[0]
            except IndexError:
                pass

        columns = ('item', 'sites', 'groups', 'lock_date', 'expiration_date', 'user', 'dn', 'service_id', 'comment')

        comment = None
        if 'comment' in request:
            comment = request['comment']

        values = [(request['item'], None, None, MySQL.bare('NOW()'), MySQL.bare('FROM_UNIXTIME(%d)' % request['expires']), user, dn, service_id, comment)]
        if 'sites' in request:
            new_values = []
            for site in request['sites']:
                for v in values:
                    new_values.append(v[:1] + (site,) + v[2:])
            values = new_values
        if 'groups' in request:
            new_values = []
            for group in request['groups']:
                for v in values:
                    new_values.append(v[:2] + (group,) + v[3:])
            values = new_values

        new_locks = []

        for v in values:
            lock_id = self.registry.db.insert_get_id('detox_locks', columns, v)

            new_lock = {
                'lockid': lock_id,
                'user': user,
                'dn': dn,
                'item': request['item'],
                'locked': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                'expires': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(request['expires']))
            }
            if v[7] != 0:
                new_lock['service'] = request['service']
            if v[1] is not None:
                new_lock['sites'] = v[1]
            if v[2] is not None:
                new_lock['groups'] = v[2]
            if 'comment' in request:
                new_lock['comment'] = request['comment']

            new_locks.append(new_lock)

        return new_locks

    def _update_lock(self, existing, request):
        updates = []
        args = []
        if 'expires' in request:
            updates.append('`expiration_date` = FROM_UNIXTIME(%s)')
            args.append(request['expires'])
        if 'comment' in request:
            updates.append('`comment` = %s')
            args.append(request['comment'])

        if len(updates) == 0:
            return []

        sql = 'UPDATE `detox_locks` SET ' + ', '.join(updates)

        updated = []

        for lock in existing:
            self.registry.db.query(sql + ' WHERE `id` = %d' % lock['lockid'], *args)

            if 'expires' in request:
                lock['expires'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(request['expires']))
            if 'comment' in request:
                lock['comment'] = request['comment']

            updated.append(lock)

        return updated

    def _disable_lock(self, existing):
        history_sql = 'INSERT INTO `detox_locks` (`id`, `item`, `sites`, `groups`, `lock_date`, `unlock_date`, `expiration_date`, `user_id`, `service_id`, `comment`)'
        history_sql += ' VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s), NOW(), FROM_UNIXTIME(%s), %s, %s, %s)'

        registry_sql = 'DELETE FROM `detox_locks` WHERE `id` = %s'

        disabled = []

        for lock in existing:
            if 'unlocked' in lock:
                continue
            
            user_id = self.history.save_users([(lock['user'], lock['dn'])], get_ids = True)[0]

            if 'sites' in lock:
                sites = lock['sites']
            else:
                sites = None

            if 'groups' in lock:
                groups = lock['groups']
            else:
                groups = None

            if 'service' in lock:
                service_id = self.history.save_user_services([lock['service']], get_ids = True)[0]
            else:
                service_id = 0

            if 'comment' in lock:
                comment = lock['comment']
            else:
                comment = None

            lock_date = calendar.timegm(time.strptime(lock['locked'], '%Y-%m-%d %H:%M:%S %Z'))
            expiration_date = calendar.timegm(time.strptime(lock['expires'], '%Y-%m-%d %H:%M:%S %Z'))

            self.history.db.query(history_sql, lock['lockid'], lock['item'], sites, groups,
                lock_date, expiration_date, user_id, service_id, comment)

            disabled.append(lock)
            
            self.registry.db.query(registry_sql, lock['lockid'])

        return disabled

    def _lock_tables(self):
        self.registry.db.lock_tables(write = ['detox_locks', ('detox_locks', 'l'), 'user_services', ('user_services', 's')])

    def _unlock_tables(self):
        self.registry.db.unlock_tables()


class DetoxLock(DetoxLockBase):
    def __init__(self, config):
        DetoxLockBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        self._validate_request(request, inventory, ['item', 'expires'], ['service', 'sites', 'groups', 'comment'])

        request['user'] = (caller.name,)

        self._lock_tables()
        try:
            existing = self._get_lock(request, valid_only = True)
    
            if len(existing) == 0:
                # new lock
                locks = self._create_lock(request, caller.name, caller.dn)
                self.message = 'Lock created'
    
            else:
                locks = self._update_lock(existing, request)
                self.message = 'Lock updated'

        finally:
            self._unlock_tables()

        return locks


class DetoxUnlock(DetoxLockBase):
    def __init__(self, config):
        DetoxLockBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        self._validate_request(request, inventory, [], ['service', 'lockid', 'item', 'sites', 'groups', 'created_before', 'created_after', 'expires_before', 'expires_after'])

        request['user'] = (caller.name,)

        self._lock_tables()
        try:
            existing = self._get_lock(request, valid_only = True)
    
            if len(existing) == 0:
                self.message = 'No lock found'
                locks = None
    
            else:
                locks = self._disable_lock(existing)
    
                self.message = 'Unlocked'

        finally:
            self._unlock_tables()

        return locks


class DetoxListLock(DetoxLockBase):
    def run(self, caller, request, inventory):
        self._validate_request(request, inventory, [], ['lockid', 'user', 'service', 'item', 'sites', 'groups', 'created_before', 'created_after', 'expires_before', 'expires_after', 'showall'])

        if 'user' not in request:
            request['user'] = (caller.name,)

        valid_only = (not yesno(request, 'showall', False))

        existing = self._get_lock(request, valid_only = valid_only)

        self.message = '%d locks found' % len(existing)
        return existing


class DetoxLockSet(DetoxLockBase):
    def __init__(self, config):
        DetoxLockBase.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        if type(self.input_data) is not list:
            raise IllFormedRequest('input', type(self.input_data).__name__, hint = 'data must be a list')

        to_insert = []

        self._lock_tables()
        try:
            constraint = {'user': (caller.name,)}
            if 'service' in request:
                constraint['service'] = request['service']
    
            user_all = self._get_lock(constraint, valid_only = True)
    
            found_ids = set()
            n_inserted = 0
    
            # Validate and format all requests first (modifies in-place)
            for entry in self.input_data:
                self._validate_request(entry, inventory, ['item', 'expires'], ['sites', 'groups', 'comment'])
                if 'service' in request:
                    entry['service'] = request['service']
    
            for entry in self.input_data:
                existing = self._get_lock(entry, valid_only = True)
    
                if len(existing) == 0:
                    n_inserted += len(self._create_lock(entry, caller.name, caller.dn))
                else:
                    found_ids.update(e['lockid'] for e in existing)
                    self._update_lock(existing, entry)
    
            to_unlock = set(e['lockid'] for e in user_all) - found_ids
            n_disabled = len(self._disable_lock(dict(('lockid', i) for i in to_unlock)))

        finally:
            self._unlock_tables()

        self.message = '%d locks' % (len(user_all) + n_inserted - n_disabled)

        return None


class DetoxLockHelp(WebModule, HTMLMixin):
    """
    Show a help webpage
    """

    def __init__(self, config):
        WebModule.__init__(self, config) 
        HTMLMixin.__init__(self, 'Dynamo Detox locks API', 'detox/locks_help.html')

    def run(self, caller, request, inventory):
        return self.form_html({})


export_data = {
    'lock/lock': DetoxLock,
    'lock/unlock': DetoxUnlock,
    'lock/list': DetoxListLock,
    'lock/set': DetoxLockSet
}

export_web = {
    'lock/help': DetoxLockHelp
}
