import time
import calendar
import json

from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin
from dynamo.web.modules._userdata import UserDataMixin
import dynamo.dataformat as df

class CopyRequest(object):
    """
    Utility class to carry around all relevant information about a request.
    """

    def __init__(self, request_id, group, n, status, first_request, last_request, request_count, reject_reason, user, user_dn):
        self.request_id = request_id
        self.group = group
        self.n = n
        self.status = status
        self.first_request = first_request
        self.last_request = last_request
        self.request_count = request_count
        self.reject_reason = reject_reason
        self.user = user
        self.user_dn = user_dn
        self.sites = []
        self.items = []
        self.active_copies = None

    def to_dict(self):
        d = {'request_id': self.request_id,
             'item': self.items,
             'site': self.sites,
             'group': self.group,
             'n': self.n,
             'status': self.status,
             'first_request': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.first_request)),
             'last_request': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.last_request)),
             'request_count': self.request_count,
             'user': self.user,
             'dn': self.user_dn
        }

        if self.status == 'rejected':
            d['reason'] = self.reject_reason
        elif self.status in ('activated', 'completed'):
            active = d['copy'] = []
            # active_copies must be non-null
            for item, site, status, update in self.active_copies:
                active.append({'item': item, 'site': site, 'status': status, 'updated': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(update))})
        
        return d

class CopyRequestMixin(UserDataMixin):
    """
    A mixin defining methods common to Make, Poll, and Cancel.
    """

    def __init__(self):
        UserDataMixin.__init__(self)

        self.request = {}
        self.existing = {}

        self.user_info_cache = {} # {name: (id, dn), id: (name, dn)}

    def parse_input(self, request, inventory, allowed_fields, required_fields = tuple()):
        # Check we have the right request fields

        input_fields = set(request.keys())
        allowed_fields = set(allowed_fields)
        excess = input_fields - allowed_fields
        if len(excess) != 0:
            raise ExtraParameter(list(excess)[0])

        for key in required_fields:
            if key not in request:
                raise MissingParameter(key)

        # Pick up the values and cast them to correct types

        for key in ['request_id', 'n']:
            if key not in request:
                continue

            try:
                self.request[key] = int(request[key])
            except ValueError:
                raise IllFormedRequest(key, request[key], hint = '%s must be an integer' % key)

        for key in ['item', 'status', 'site', 'user']:
            if key not in request:
                continue

            value = request[key]
            if type(value) is str:
                self.request[key] = value.strip().split(',')
            elif type(value) is list:
                self.request[key] = value
            else:
                raise IllFormedRequest(key, request[key], hint = '%s must be a string or a list' % key)

        for key in ['group']:
            if key not in request:
                continue

            self.request[key] = request[key]

        # Check value validity
        # We check the site, group, and item names but not use their ids in the table.
        # The only reason for this would be to make the registry not dependent on specific inventory store technology.

        if 'item' in self.request:
            for item in self.request['item']:
                if item in inventory.datasets:
                    # OK this is a known dataset
                    continue
    
                try:
                    dataset_name, block_name = df.Block.from_full_name(item)
                except df.ObjectError:
                    raise InvalidRequest('Invalid item name %s' % item)
    
                try:
                    inventory.datasets[dataset_name].find_block(block_name, must_find = True)
                except:
                    raise InvalidRequest('Invalid block name %s' % item)

        if 'site' in self.request:
            for site in self.request['site']:
                try:
                    inventory.sites[site]
                except KeyError:
                    raise InvalidRequest('Invalid site name %s' % site)

        if 'group' in self.request:
            try:
                inventory.groups[self.request['group']]
            except KeyError:
                raise InvalidRequest('Invalid group name %s' % self.request['group'])

        # Minor security concern: do we want to expose the user list this way?
        if 'user' in self.request:
            for uname in self.request['user']:
                result = self.authorizer.identify_user(name = uname)
                if result is None:
                    raise InvalidRequest('Invalid user name %s' % uname)

                self.user_info_cache[uname] = result[1:] # id, dn
                self.user_info_cache[result[1]] = (uname, result[2]) # name, dn

        if 'status' in self.request:
            for status in self.request['status']:
                if status not in ('new', 'activated', 'completed', 'rejected', 'cancelled'):
                    raise InvalidRequest('Invalid status value %s' % status)

    def lock_tables(self):
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [('copy_requests', 'r'), 'copy_requests', ('active_copies', 'a'), 'active_copies']
        if 'item' in self.request or 'site' in self.request:
            tables.extend([('copy_request_items', 'i'), 'copy_request_items', ('copy_request_sites', 's'), 'copy_request_sites'])

        self.registry.lock_tables(tables)

    def load_existing(self, by_id = False):
        """
        Find an existing copy request from values in self.request and set self.existing.
        If request_id is set but no existing record is found, raises an InvalidRequest error.
        """
        constraints = []
        args = []
        if 'request_id' in self.request:
            constraints.append('r.`id` = %s')
            args.append(self.request['request_id'])

        if not by_id:
            if 'status' in self.request:
                constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in self.request['status']))
            elif 'request_id' not in self.request:
                # limit to live requests
                constraints.append('r.`status` IN (\'new\', \'activated\')')
    
            if 'user' in self.request:
                constraints.append('r.`user_id` IN (%s)' % ','.join('%d' % self.user_info_cache[user][0] for user in self.request['user']))
    
            if 'item' in self.request or 'site' in self.request:
                self.make_temp_table()
                constraints.append('r.`id` IN (SELECT `id` FROM `copy_ids_tmp`)')

        self.existing = self.fill_from_sql(constraints, args)

    def fill_from_sql(self, constraints, args):
        requests = {}

        sql = 'SELECT r.`id`, r.`group`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`first_request_time`), UNIX_TIMESTAMP(r.`last_request_time`),'
        sql += ' r.`request_count`, r.`rejection_reason`, r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)
        sql += ' ORDER BY r.`id`'

        _rid = 0
        for rid, group, n, status, first_request, last_request, count, reason, user_id, a_item, a_site, a_status, a_update in self.registry.xquery(sql, *tuple(args)):
            if rid != _rid:
                _rid = rid

                try:
                    user, dn = self.user_info_cache[user_id]
                except KeyError:
                    result = self.authorizer.identify_user(uid = user_id)
                    if result is None:
                        user = None
                        dn = None
                    else:
                        user, dn = self.user_info_cache[user_id] = (result[0], result[2])

                request = requests[rid] = CopyRequest(rid, group, n, status, first_request, last_request, count, reason, user, dn)

            if a_item is not None:
                if request.active_copies is None:
                    request.active_copies = []

                request.active_copies.append((a_item, a_site, a_status, a_update))

        if len(requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, s.`site` FROM `copy_request_sites` AS s WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in requests.iterkeys())
            for rid, site in self.registry.xquery(sql):
                requests[rid].sites.append(site)
    
            # get the items
            sql = 'SELECT i.`request_id`, i.`item` FROM `copy_request_items` AS i WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in requests.iterkeys())
            for rid, item in self.registry.xquery(sql):
                requests[rid].items.append(item)

        return requests


class MakeCopyRequest(WebModule, MySQLRegistryMixin, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        CopyRequestMixin.__init__(self)

        # config.request.copy points to the "copy" method of dict
        self.default_group = config['request']['copy']['default_group']

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'group', 'n'))

        self.lock_tables()

        try:
            if 'request_id' in self.request:
                self.load_existing(by_id = True)
                if len(self.existing) == 0:
                    raise InvalidRequest('Invalid request id %d' % self.request['request_id'])

                existing = self.existing[self.request['request_id']]
                if existing.status != 'new':
                    raise InvalidRequest('Request %d is already activated and therefore cannot be updated' % self.request['request_id'])

                # update the existing request
                requests = self.update_request(existing)

            else:
                # create a new request
                requests = self.create_request(caller)

        finally:
            self.registry.unlock_tables()

        # requests is a single-element dictionary
        return [r.to_dict() for r in requests.itervalues()]

    def create_request(self, caller):
        if 'n' not in self.request:
            self.request['n'] = 1

        if 'group' not in self.request:
            self.request['group'] = self.default_group

        sql = 'INSERT INTO `copy_requests` (`group`, `num_copies`, `user_id`, `first_request_time`, `last_request_time`) VALUES (%s, %s, %s, NOW(), NOW())'
        self.registry.query(sql, self.request['group'], self.request['n'], caller.id)

        request_id = self.registry.last_insert_id

        fields = ('request_id', 'site')
        mapping = lambda site: (request_id, site)
        self.registry.insert_many('copy_request_sites', fields, mapping, self.request['site'])
        sql = 'INSERT INTO `copy_request_sites` (`request_id`, `site`) VALUES (%s, %s)'
        
        fields = ('request_id', 'item')
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('copy_request_items', fields, mapping, self.request['item'])

        constraints = ['r.`id` = %s']
        args = [request_id]

        return self.fill_from_sql(constraints, args)

    def update_request(self, existing):
        # The only updatable fields are group, num_copies, and status (to cancelled)
        # Requests are only updatable in the new state

        if 'group' in self.request:
            existing.group = self.request['group']
        if 'n' in self.request:
            existing.n = self.request['n']
        if 'status' in self.request:
            existing.status = self.request['status']

        sql = 'UPDATE `copy_requests` SET `group` = %s, `num_copies` = %s, `status` = %s, `last_request_time` = NOW() WHERE `request_id` = %s'
        self.registry.query(sql, existing.group, existing.n, existing.status, existing.request_id)

        return {existing.request_id: existing}


class PollCopyRequest(WebModule, MySQLRegistryMixin, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        CopyRequestMixin.__init__(self)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))

        self.load_existing()

        return [r.to_dict() for r in self.existing.itervalues()]


class CancelCopyRequest(WebModule, MySQLRegistryMixin, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        CopyRequestMixin.__init__(self)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id',), ('request_id',))

        self.lock_tables()
        
        try:
            self.load_existing(by_id = True)
            if len(self.existing) == 0:
                raise InvalidRequest('Invalid request id %d' % self.request['request_id'])
                
            existing = self.existing[self.request['request_id']]
            if existing.status == 'new':
                existing.status = 'cancelled'
                sql = 'UPDATE `copy_requests` SET `status` = \'cancelled\' WHERE `request_id` = %s'
                self.registry.query(sql, existing.request_id)

            elif existing.status == 'cancelled':
                pass

            else:
                raise InvalidRequest('Request %d cannot be cancelled any more' % self.request['request_id'])

        finally:
            self.registry.unlock_tables()

        return existing.to_dict()


export_data = {
    'copy': MakeCopyRequest,
    'pollcopy': PollCopyRequest,
    'cancelcopy': CancelCopyRequest
}
