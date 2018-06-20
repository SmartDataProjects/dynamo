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

class DeletionRequest(object):
    """
    Utility class to carry around all relevant information about a request.
    """

    def __init__(self, request_id, user, user_dn, status, request_time, rejection_reason = None):
        self.request_id = request_id
        self.user = user
        self.user_dn = user_dn
        self.status = status
        self.request_time = request_time
        self.reject_reason = reject_reason
        self.sites = []
        self.items = []
        self.active_deletions = None

    def to_dict(self):
        d = {
            'request_id': self.request_id,
            'item': self.items,
            'site': self.sites,
            'status': self.status,
            'request_time': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(self.request_time)),
            'user': self.user,
            'dn': self.user_dn
        }

        if self.status == 'rejected':
            d['reason'] = self.reject_reason
        elif self.status in ('activated', 'completed'):
            active = d['deletion'] = []
            # active_copies must be non-null
            for item, site, status, update in self.active_deletions:
                active.append({'item': item, 'site': site, 'status': status, 'updated': time.strftime('%Y-%m-%dT%H:%M:%S UTC', time.gmtime(update))})
        
        return d


class DeletionRequestMixin(ParseInputMixin, SaveParamsMixin, MySQLRegistryMixin, MySQLHistoryMixin):
    def __init__(self, config):
        ParseInputMixin.__init__(self, config)
        SaveParamsMixin.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        MySQLHistoryMixin.__init__(self, config)

    def lock_tables(self):
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('deletion_requests', 'r'), 'deletion_requests', ('active_deletions', 'a'), 'active_deletions',
            ('deletion_request_items', 'i'), 'deletion_request_items', ('deletion_request_sites', 's'), 'deletion_request_sites'
        ]

        self.registry.lock_tables(write = tables)

    def fill_from_sql(self, request_id = None, status = None, user = None, item = None, site = None):
        live_requests = {}

        sql = 'SELECT r.`id`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `deletion_requests` AS r'
        sql += ' LEFT JOIN `active_deletions` AS a ON a.`request_id` = r.`id`'

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if status is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in status))
        if user is not None:
            constraints.append('r.`user_id` IN (%s)' % ','.join('%d' % self.user_info_cache[u][0] for u in user))
        if item is not None or site is not None:
            self.make_temp_registry_tables('deletion', item, site)
            constraints.append('r.`id` IN (SELECT `id` FROM `{0}`.`ids_tmp`)'.format(self.registry.scratch_db))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        _rid = 0
        for rid, status, request_time, user_id, a_item, a_site, a_status, a_update in self.registry.xquery(sql):
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

                request = live_requests[rid] = DeletionRequest(rid, user, dn, status, request_time)

            if a_item is not None:
                if request.active_deletions is None:
                    request.active_deletions = []

                request.active_deletions.append((a_item, a_site, a_status, a_update))

        if len(live_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, s.`site` FROM `deletion_request_sites` AS s WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in live_requests.iterkeys())
            for rid, site in self.registry.xquery(sql):
                live_requests[rid].sites.append(site)
    
            # get the items
            sql = 'SELECT i.`request_id`, i.`item` FROM `deletion_request_items` AS i WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in live_requests.iterkeys())
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

        sql = 'SELECT r.`id`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`rejection_reason`, u.`name`, u.`dn`'
        sql += ' FROM `deletion_requests` AS r'
        sql += ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`'

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if status is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in status))
        if user is not None:
            constraints.append('r.`user_id` IN (SELECT `id` FROM `users` WHERE `name` IN (%s))' % ','.join('\'%s\'' % u for u in user))
        if item is not None or site is not None:
            self.make_temp_history_tables('deletion', item is not None, site)
            constraints.append('r.`id` IN (SELECT `id` FROM `{0}`.`ids_tmp`)'.format(self.history.scratch_db))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        for rid, status, request_time, reason, user, dn in self.history.xquery(sql):
            if rid not in live_requests:
                archived_requests[rid] = DeletionRequest(rid, user, dn, status, request_time, reason)

        if len(archived_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, h.`name` FROM `deletion_request_sites` AS s'
            sql += ' INNER JOIN `sites` AS h ON h.`id` = s.`site_id`'
            sql += ' WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, site in self.history.xquery(sql):
                archived_requests[rid].sites.append(site)
    
            # get the datasets
            sql = 'SELECT i.`request_id`, d.`name` FROM `deletion_request_datasets` AS i'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = i.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset in self.history.xquery(sql):
                archived_requests[rid].items.append(dataset)

            # get the blocks
            sql = 'SELECT i.`request_id`, d.`name`, b.`name` FROM `deletion_request_blocks` AS i'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = i.`block_id`'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset, block in self.history.xquery(sql):
                archived_requests[rid].items.append(df.Block.to_full_name(dataset, block))

        # there should not be any overlap of request ids
        all_requests = live_requests
        all_requests.update(archived_requests)

        return all_requests


class MakeDeletionRequest(WebModule, DeletionRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        DeletionRequestMixin.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('item', 'site'), ('item', 'site'))

        self.lock_tables()

        try:
            self.load_existing()

            existing = None

            for request_id in sorted(self.existing.iterkeys()):
                if self.existing[request_id].status == 'new':
                    existing = self.existing[request_id]
                    break
                elif self.existing[request_id].status == 'activated':
                    existing = self.existing[request_id]

            if existing is not None:
                return [existing]

            else:
                now = int(time.time())
        
                # Make an entry in registry
                columns = ('user_id', 'request_time')
                values = (caller.id, MySQL.bare('FROM_UNIXTIME(%d)' % now))
                request_id = self.registry.insert_get_id('deletion_requests', columns, values)
        
                fields = ('request_id', 'site')
                mapping = lambda site: (request_id, site)
                self.registry.insert_many('deletion_request_sites', fields, mapping, self.request['site'])
        
                fields = ('request_id', 'item')
                mapping = lambda item: (request_id, item)
                self.registry.insert_many('deletion_request_items', fields, mapping, self.request['item'])
        
                # Make an entry in history
                self.save_params(caller)
        
                sql = 'INSERT INTO `deletion_requests` (`id`, `user_id`, `request_time`)'
                sql += ' SELECT %s, u.`id`, FROM_UNIXTIME(%s) FROM `users` AS u'
                sql += ' WHERE u.`dn` = %s'
                self.history.query(sql, request_id, now, caller.dn)
        
                self.history.insert_select_many('deletion_request_sites', ('request_id', 'site_id'), 'sites', (MySQL.bare('%d' % request_id), 'id'), 'name', self.request['site'])
        
                # history_dataset_names set in save_params
                self.history.insert_select_many('deletion_request_datasets', ('request_id', 'dataset_id'), 'datasets', (MySQL.bare('%d' % request_id), 'id'), 'name', self.history_dataset_names)
        
                # history_block_names set in save_params
                self.history.insert_select_many('deletion_request_blocks', ('request_id', 'block_id'), 'blocks', (MySQL.bare('%d' % request_id), 'id'), ('dataset_id', 'name'), self.history_block_names)
        
                requests = self.fill_from_sql(request_id = request_id)

                # requests is a single-element dictionary
                return [r.to_dict() for r in requests.itervalues()]


class PollDeletionRequest(WebModule, DeletionRequestMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        DeletionRequestMixin.__init__(self, config)

    def run(self, caller, request, inventory):
        self.parse_input(request, inventory, ('request_id', 'item', 'site', 'status', 'user'))
    
        self.load_existing()

        return [r.to_dict() for r in self.existing.itervalues()]


export_data = {
    'delete': MakeDeletionRequest,
    'polldelete': PollDeletionRequest
}
