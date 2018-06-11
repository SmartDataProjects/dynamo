from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin
from dynamo.web.modules._userdata import UserDataMixin
import dynamo.dataformat as df

class CopyRequestMixin(UserDataMixin):
    class CopyReuquest(object):
        def __init__(self, request_id, group, n, status, first_request, last_request, request_count, reject_reason, user):
            self.request_id = request_id
            self.group = group
            self.n = n
            self.status = status
            self.first_request = first_request
            self.last_request = last_request
            self.request_count = request_count
            self.reject_reason = reject_reason
            self.user = user
            self.sites = []
            self.items = []
            self.active_copies = None

        def to_json(self):
            pass

    def __init__(self):
        UserDataMixin.__init__(self)

        self.request = {}
        self.existing = None

        self.table_locked = False

    def parse_input(self, request, allowed_fields):
        input_fields = set(request.keys())
        allowed_fields = set(allowed_fields)
        excess = input_fields - allowed_fields
        if len(excess) != 0:
            raise ExtraParameter(list(excess)[0])

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

        for item in self.request['item']:
            if df.Dataset.name_pattern.match(item):
                continue
            try:
                df.Block.from_full_name(item)
            except df.ObjectError:
                raise InvalidRequest('Invalid item name %s' % item)

        for status in self.request['status']:
            if status not in ('new', 'activated', 'updated', 'completed', 'rejected', 'cancelled'):
                raise InvalidRequest('Invalid status value %s' % status)

    def load_existing(self, lock = False)
        """
        Find an existing copy request from values in self.request and set self.existing.
        If request_id is set but no existing record is found, raises an InvalidRequest error.
        """

        if 'user' in self.request and 'user_id' not in self.request:
            self.request['user_id'] = {}
            for uname in self.request['user']:
                result = self.authorizer.identify_user(name = uname, with_id = True)
                if result is not None:
                    self.request['user_id'][result[1]] = uname

        if lock:
            tables = ['`copy_requests` AS r', '`active_copies` AS a']
            if 'item' in self.request or 'site' in self.request:
                tables.append('`copy_request_items` AS i')
                tables.append('`copy_request_sites` AS s')
            self.lock_tables(tables)

        sql = 'SELECT r.`id`, r.`group`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`first_request_time`), UNIX_TIMESTAMP(r.`last_request_time`),'
        sql += ' r.`request_count`, r.`rejection_reason`, r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'

        constraints = []
        args = []
        if 'request_id' in self.request:
            constraints.append('r.`id` = %s')
            args.append(self.request['request_id'])

        if 'status' in self.request:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in self.request['status']))
        elif 'request_id' not in self.request:
            # limit to live requests
            constraints.append('r.`status` IN (\'new\', \'activated\', \'updated\')')

        if 'user_id' in self.request:
            constraints.append('r.`user_id` IN (%s)' % ','.join('%d' % d for d in self.request['user_id'].iterkeys()))

        if 'item' in self.request or 'site' in self.request:
            self.make_temp_table()
            constraints.append('r.`id` IN (SELECT `id` FROM `copy_ids_tmp`)')

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        self.existing = {}

        _rid = 0
        for rid, group, n, status, first_request, last_request, count, reason, user_id, a_item, a_site, a_status, a_update from self.register.xquery(sql, *tuple(args)):
            if rid != _rid:
                _rid = rid

                if 'user_id' in self.request:
                    user = self.request['user_id'][user_id]
                else:
                    user = None

                request = self.existing[rid] = CopyRequest(rid, group, n, status, first_request, last_request, count, reason, user)

            if a_item is not None:
                if request.active_copies is None:
                    request.active_copies = []

                request.active_copies.append((a_item, a_site, a_status, a_update))

        # get the sites
        sql = 'SELECT s.`request_id`, s.`site` FROM `copy_request_sites` AS s WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in self.existing.iterkeys())
        for rid, site in self.registry.xquery(sql):
            self.existing[rid].sites.append(site)

        # get the items
        sql = 'SELECT i.`request_id`, i.`site` FROM `copy_request_items` AS i WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in self.existing.iterkeys())
        for rid, item in self.registry.xquery(sql):
            self.existing[rid].items.append(item)

    def lock_tables(self, write = [], read = []):
        if len(write) == 0 and len(read) == 0:
            return

        sql = 'LOCK TABLES '
        if len(write) != 0:
            sql += ', '.join('%s WRITE' % table for table in write)
        if len(read) != 0:
            if len(write) != 0:
                sql += ', '
            sql += ', '.join('%s READ' % table for table in read)
            
        self.registry.query(sql)

        self.table_locked = True

    def unlock_tables(self):
        if self.table_locked:
            self.registry.query('UNLOCK TABLES')

class MakeCopyRequest(WebModule, MySQLRegistryMixin, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        CopyRequestMixin.__init__(self)

        self.default_group = config.request.copy.default_group

    def run(self, caller, request, inventory):
        self.parse_input(request, ('request_id', 'item', 'site', 'group', 'n'))
        
        try:
            self.load_existing(lock = True)
    
            if self.existing is None:
                # new request
                if 'n' not in self.request:
                    self.request['n'] = 1
    
                if 'group' not in self.request:
                    self.request['group'] = self.default_group
    
                return self.create_request()
    
            else:
                return self.update_request()

        finally:
            self.unlock_tables()

    def create_request(self):
        pass

    def update_request(self):
        pass


export_data = {
    'copy': MakeCopyRequest,
    'pollcopy': PollCopyRequest,
    'cancelcopy': CancelCopyRequest
}
