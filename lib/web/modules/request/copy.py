import time
import calendar
import json
import logging

from dynamo.web.exceptions import InvalidRequest
from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin
from dynamo.web.modules._mysqlhistory import MySQLHistoryMixin
from dynamo.web.midules.request.mixin import ParseInputMixin, SaveParamsMixin
from dynamo.utils.interface.mysql import MySQL
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class CopyRequest(object):
    """
    Utility class to carry around all relevant information about a request.
    """

    def __init__(self, request_id, user, user_dn, group, n, status, first_request, last_request, request_count, reject_reason = None):
        self.request_id = request_id
        self.user = user
        self.user_dn = user_dn
        self.group = group
        self.n = n
        self.status = status
        self.first_request = first_request
        self.last_request = last_request
        self.request_count = request_count
        self.reject_reason = reject_reason
        self.sites = []
        self.items = []
        self.active_copies = None

    def to_dict(self):
        d = {
            'request_id': self.request_id,
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


class CopyRequestMixin(MySQLRegistryMixin, MySQLHistoryMixin, ParseInputMixin, SaveParamsMixin):
    """
    A mixin defining methods common to Make, Poll, and Cancel.
    Requests are written in registry when they are in new and activated states.
    When moving to terminal states (completed, rejected, cancelled) the records are migrated to history.
    """

    def __init__(self, config):
        MySQLRegistryMixin.__init__(self, config)
        MySQLHistoryMixin.__init__(self, config)
        ParseInputMixin.__init__(self, config)
        SaveParamsMixin.__init__(self, config)

        # we'll be using temporary tables
        self.registry.reuse_connection = True
        self.history.reuse_connection = True

    def lock_tables(self):
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('copy_requests', 'r'), 'copy_requests', ('active_copies', 'a'), 'active_copies',
            ('copy_request_items', 'i'), 'copy_request_items', ('copy_request_sites', 's'), 'copy_request_sites'
        ]

        self.registry.lock_tables(write = tables)

    def fill_from_sql(self, request_id = None, status = None, user = None, item = None, site = None): #override
        live_requests = {}

        sql = 'SELECT r.`id`, r.`group`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`first_request_time`), UNIX_TIMESTAMP(r.`last_request_time`),'
        sql += ' r.`request_count`, r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if status is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in status))
        if user is not None:
            constraints.append('r.`user_id` IN (%s)' % ','.join('%d' % self.user_info_cache[u][0] for u in user))
        if item is not None or site is not None:
            self.make_temp_registry_tables('copy', item, site)
            constraints.append('r.`id` IN (SELECT `id` FROM `{0}`.`ids_tmp`)'.format(self.registry.scratch_db))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        _rid = 0
        for rid, group, n, status, first_request, last_request, count, user_id, a_item, a_site, a_status, a_update in self.registry.xquery(sql):
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

                request = live_requests[rid] = CopyRequest(rid, user, dn, group, n, status, first_request, last_request, count)

            if a_item is not None:
                if request.active_copies is None:
                    request.active_copies = []

                request.active_copies.append((a_item, a_site, a_status, a_update))

        if len(live_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, s.`site` FROM `copy_request_sites` AS s WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in live_requests.iterkeys())
            for rid, site in self.registry.xquery(sql):
                live_requests[rid].sites.append(site)
    
            # get the items
            sql = 'SELECT i.`request_id`, i.`item` FROM `copy_request_items` AS i WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in live_requests.iterkeys())
            for rid, item in self.registry.xquery(sql):
                live_requests[rid].items.append(item)

            if request_id is not None:
                # we've found the request already
                return live_requests

        if status is not None and set(status) < set(['new', 'activated']):
            # there's nothing in the archive
            return live_requests

        self.save_params()

        archived_requests = {}

        sql = 'SELECT r.`id`, g.`name`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`rejection_reason`, u.`name`, u.`dn`'
        sql += ' FROM `copy_requests` AS r'
        sql += ' INNER JOIN `groups` AS g ON g.`id` = r.`group_id`'
        sql += ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`'

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if status is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in status))
        if user is not None:
            constraints.append('r.`user_id` IN (SELECT `id` FROM `users` WHERE `name` IN (%s))' % ','.join('\'%s\'' % u for u in user))
        if item is not None or site is not None:
            self.make_temp_history_tables('copy', item is not None, site)
            constraints.append('r.`id` IN (SELECT `id` FROM `{0}`.`ids_tmp`)'.format(self.history.scratch_db))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        for rid, group, n, status, request_time, reason, user, dn in self.history.xquery(sql):
            if rid not in live_requests:
                archived_requests[rid] = CopyRequest(rid, user, dn, group, n, status, request_time, request_time, 1, reason)

        if len(archived_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, h.`name` FROM `copy_request_sites` AS s'
            sql += ' INNER JOIN `sites` AS h ON h.`id` = s.`site_id`'
            sql += ' WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, site in self.history.xquery(sql):
                archived_requests[rid].sites.append(site)
    
            # get the datasets
            sql = 'SELECT i.`request_id`, d.`name` FROM `copy_request_datasets` AS i'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = i.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset in self.history.xquery(sql):
                archived_requests[rid].items.append(dataset)

            # get the blocks
            sql = 'SELECT i.`request_id`, d.`name`, b.`name` FROM `copy_request_blocks` AS i'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = i.`block_id`'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset, block in self.history.xquery(sql):
                archived_requests[rid].items.append(df.Block.to_full_name(dataset, block))

        # there should not be any overlap of request ids
        all_requests = live_requests
        all_requests.update(archived_requests)

        return all_requests


class MakeCopyRequest(WebModule, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        CopyRequestMixin.__init__(self, config)

        # config.request.copy points to the "copy" method of dict
        self.default_group = config['request']['copy']['default_group']
        self.default_site = config['request']['copy'].get('default_site', [])

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'group', 'n'))

        self.lock_tables()

        try:
            existing = None

            if 'request_id' in self.request:
                self.load_existing(by_id = True)

                if len(self.existing) == 0:
                    raise InvalidRequest('Invalid request id %d' % self.request['request_id'])

                existing = self.existing[self.request['request_id']]

                if existing.status != 'new':
                    raise InvalidRequest('Request %d cannot be updated any more' % self.request['request_id'])

            else:
                self.load_existing()

                for request_id in sorted(self.existing.iterkeys()):
                    if self.existing[request_id].status == 'new':
                        existing = self.existing[request_id]
                        break
                    elif self.existing[request_id].status == 'activated':
                        existing = self.existing[request_id]

            if existing is None:
                # create a new request
                requests = self.create_request(inventory, caller)

            else:
                existing.request_count += 1
                existing.last_request = time.time()

                if existing.status == 'new':
                    # allow update of values
                    if 'group' in self.request:
                        existing.group = self.request['group']
                    if 'n' in self.request:
                        existing.n = self.request['n']

                requests = self.update_request(existing)

        finally:
            self.registry.unlock_tables()

        # requests is a single-element dictionary
        return [r.to_dict() for r in requests.itervalues()]

    def create_request(self, inventory, caller):
        if 'item' not in self.request:
            raise MissingParameter('item')

        if 'n' not in self.request:
            self.request['n'] = 1

        if 'group' not in self.request:
            self.request['group'] = self.default_group

        if 'site' not in self.request:
            if len(self.default_site) == 0:
                raise MissingParameter('site')
            else:
                self.request['site'] = list(self.default_site)

        now = int(time.time())

        # Make an entry in registry
        columns = ('group', 'num_copies', 'user_id', 'first_request_time', 'last_request_time')
        values = (self.request['group'], self.request['n'], caller.id, MySQL.bare('FROM_UNIXTIME(%d)' % now), MySQL.bare('FROM_UNIXTIME(%d)' % now))
        request_id = self.registry.insert_get_id('copy_requests', columns, values)

        fields = ('request_id', 'site')
        mapping = lambda site: (request_id, site)
        self.registry.insert_many('copy_request_sites', fields, mapping, self.request['site'])

        fields = ('request_id', 'item')
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('copy_request_items', fields, mapping, self.request['item'])

        # Make an entry in history
        self.save_params(caller)

        sql = 'INSERT INTO `copy_requests` (`id`, `group_id`, `num_copies`, `user_id`, `request_time`)'
        sql += ' SELECT %s, g.`id`, %s, u.`id`, FROM_UNIXTIME(%s) FROM `groups` AS g, `users` AS u'
        sql += ' WHERE g.`name` = %s AND u.`dn` = %s'
        self.history.query(sql, request_id, self.request['n'], now, self.request['group'], caller.dn)

        self.history.insert_select_many('copy_request_sites', ('request_id', 'site_id'), 'sites', (MySQL.bare('%d' % request_id), 'id'), 'name', self.request['site'])

        # history_dataset_names set in save_params
        self.history.insert_select_many('copy_request_datasets', ('request_id', 'dataset_id'), 'datasets', (MySQL.bare('%d' % request_id), 'id'), 'name', self.history_dataset_names)

        # history_block_names set in save_params
        self.history.insert_select_many('copy_request_blocks', ('request_id', 'block_id'), 'blocks', (MySQL.bare('%d' % request_id), 'id'), ('dataset_id', 'name'), self.history_block_names)

        return self.fill_from_sql(request_id = request_id)

    def update_request(self, existing):
        sql = 'UPDATE `copy_requests` SET `group` = %s, `num_copies` = %s, `last_request_time` = FROM_UNIXTIME(%s), `request_count` = %s WHERE `id` = %s'
        self.registry.query(sql, existing.group, existing.n, existing.last_request, existing.request_count, existing.request_id)

        sql = 'UPDATE `copy_requests` SET `group_id` = (SELECT `id` FROM `groups` WHERE `name` = %s), `num_copies` = %s WHERE `id` = %s'
        self.history.query(sql, existing.group, existing.n, existing.request_id)

        return {existing.request_id: existing}


class PollCopyRequest(WebModule, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        CopyRequestMixin.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))

        self.load_existing()

        return [r.to_dict() for r in self.existing.itervalues()]


class CancelCopyRequest(WebModule, CopyRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        CopyRequestMixin.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id',), ('request_id',))

        request_id = self.request['request_id']

        LOG.info('Cancelling copy request %d', request_id)

        self.lock_tables()
        
        try:
            self.load_existing(by_id = True)
            if len(self.existing) == 0:
                raise InvalidRequest('Invalid request id %d' % request_id)
                
            existing = self.existing[request_id]
            if existing.status == 'new':
                existing.status = 'cancelled'
                sql = 'UPDATE `copy_requests` SET `status` = \'cancelled\' WHERE `id` = %s'
                self.history.query(sql, request_id)

                sql = 'DELETE FROM r, a, i, s USING `copy_requests` AS r'
                sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
                sql += ' INNER JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
                sql += ' INNER JOIN `copy_request_sites` AS s ON s.`request_id` = r.`id`'
                sql += ' WHERE r.`id` = %s'
                self.registry.query(sql, request_id)

            elif existing.status == 'cancelled':
                pass

            else:
                raise InvalidRequest('Request %d cannot be cancelled any more' % request_id)

        finally:
            self.registry.unlock_tables()

        return existing.to_dict()


export_data = {
    'copy': MakeCopyRequest,
    'pollcopy': PollCopyRequest,
    'cancelcopy': CancelCopyRequest
}
