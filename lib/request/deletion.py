from dynamo.request.common import RequestManager
from dynamo.utils.interface.mysql import MySQL

class DeletionRequest(object):
    """
    Utility class to carry around all relevant information about a request.
    """

    def __init__(self, request_id, user, user_dn, status, request_time, reject_reason = None):
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


class DeletionRequestManager(RequestManager):
    def __init__(self, config):
        RequestManager.__init__(self, config, 'deletion')

    def lock(self):
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('deletion_requests', 'r'), 'deletion_requests', ('active_deletions', 'a'), 'active_deletions',
            ('deletion_request_items', 'i'), 'deletion_request_items', ('deletion_request_sites', 's'), 'deletion_request_sites'
        ]

        if not self.dry_run:
            self.registry.lock_tables(write = tables)

    def get_requests(self, authorizer, request_id = None, statuses = None, users = None, items = None, sites = None): #override
        live_requests = {}

        sql = 'SELECT r.`id`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `deletion_requests` AS r'
        sql += ' LEFT JOIN `active_deletions` AS a ON a.`request_id` = r.`id`'

        user_cache = {} # reduce interaction with the authorizer

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if statuses is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in statuses))
        if users is not None:
            user_ids = []
            for user in users:
                result = authorizer.identify_user(name = user)
                if result is not None:
                    user, user_id, dn = result
                    user_cache[user_id] = (user, dn)
                    user_ids.append(user_id)
                
            constraints.append('r.`user_id` IN (%s)' % ','.join('%d' % user_ids))
        if items is not None or sites is not None:
            self.make_temp_registry_tables(items, sites)
            constraints.append('r.`id` IN (SELECT `id` FROM `{0}`.`ids_tmp`)'.format(self.registry.scratch_db))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        _rid = 0
        for rid, status, request_time, user_id, a_item, a_site, a_status, a_update in self.registry.xquery(sql):
            if rid != _rid:
                _rid = rid

                try:
                    user, dn = user_cache[user_id]
                except KeyError:
                    result = authorizer.identify_user(uid = user_id)
                    if result is None:
                        user = None
                        dn = None
                    else:
                        user, dn = user_cache[user_id] = (result[0], result[2])

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

        if statuses is not None and set(statuses) < set(['new', 'activated']):
            # there's nothing in the archive
            return live_requests

        if sites is not None:
            self.save_sites(sites)

        if items is not None:
            history_dataset_names = []
            history_block_names = []
            self.save_items(items, history_dataset_names, history_block_names)
        else:
            history_dataset_names = None
            history_block_names = None

        archived_requests = {}

        sql = 'SELECT r.`id`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`rejection_reason`, u.`name`, u.`dn`'
        sql += ' FROM `deletion_requests` AS r'
        sql += ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`'

        constraints = []
        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)
        if statuses is not None:
            constraints.append('r.`status` IN (%s)' % ','.join('\'%s\'' % s for s in statuses))
        if users is not None:
            constraints.append('r.`user_id` IN (SELECT `id` FROM `users` WHERE `name` IN (%s))' % ','.join('\'%s\'' % u for u in users))
        if items is not None or sites is not None:
            self.make_temp_history_tables(history_dataset_names, history_block_names, sites)
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

    def create_request(self, caller, items, sites):
        now = int(time.time())

        if self.dry_run:
            return DeletionRequest(0, caller.name, caller.dn, 'new', now, None)

        # Make an entry in registry
        columns = ('user_id', 'request_time')
        values = (caller.id, MySQL.bare('FROM_UNIXTIME(%d)' % now))
        request_id = self.registry.insert_get_id('deletion_requests', columns, values)

        fields = ('request_id', 'site')
        mapping = lambda site: (request_id, site)
        self.registry.insert_many('deletion_request_sites', fields, mapping, sites)

        fields = ('request_id', 'item')
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('deletion_request_items', fields, mapping, items)

        # Make an entry in history
        history_dataset_names = []
        history_block_names = []
        self.save_user(caller)
        self.save_items(items, history_dataset_names, history_block_names)
        self.save_sites(sites)

        sql = 'INSERT INTO `deletion_requests` (`id`, `user_id`, `request_time`)'
        sql += ' SELECT %s, u.`id`, FROM_UNIXTIME(%s) FROM `groups` AS g, `users` AS u'
        sql += ' WHERE u.`dn` = %s'
        self.history.query(sql, request_id, now, caller.dn)

        self.history.insert_select_many('deletion_request_sites', ('request_id', 'site_id'), 'sites', (MySQL.bare('%d' % request_id), 'id'), 'name', sites)

        # history_dataset_names set in save_params
        self.history.insert_select_many('deletion_request_datasets', ('request_id', 'dataset_id'), 'datasets', (MySQL.bare('%d' % request_id), 'id'), 'name', history_dataset_names)

        # history_block_names set in save_params
        self.history.insert_select_many('deletion_request_blocks', ('request_id', 'block_id'), 'blocks', (MySQL.bare('%d' % request_id), 'id'), ('dataset_id', 'name'), history_block_names)

        return self.get_requests(request_id = request_id)[request_id]
