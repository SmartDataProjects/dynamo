from dynamo.request.common import RequestManager
from dynamo.utils.interface.mysql import MySQL

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


class CopyRequestManager(RequestManager):
    def __init__(self, config):
        RequestManager.__init__(self, config, 'copy')

    def lock(self):
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('copy_requests', 'r'), 'copy_requests', ('active_copies', 'a'), 'active_copies',
            ('copy_request_items', 'i'), 'copy_request_items', ('copy_request_sites', 's'), 'copy_request_sites'
        ]

        if not self.dry_run:
            self.registry.lock_tables(write = tables)

    def save_group(self, group):
        if not self.dry_run:
            self.history.insert_update('groups', ('name',), group, update_columns = ('name',))

    def get_requests(self, authorizer, request_id = None, statuses = None, users = None, items = None, sites = None): #override
        live_requests = {}

        sql = 'SELECT r.`id`, r.`group`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`first_request_time`), UNIX_TIMESTAMP(r.`last_request_time`),'
        sql += ' r.`request_count`, r.`user_id`, a.`item`, a.`site`, a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'

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
        for rid, group, n, status, first_request, last_request, count, user_id, a_item, a_site, a_status, a_update in self.registry.xquery(sql):
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

        sql = 'SELECT r.`id`, g.`name`, r.`num_copies`, r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`rejection_reason`, u.`name`, u.`dn`'
        sql += ' FROM `copy_requests` AS r'
        sql += ' INNER JOIN `groups` AS g ON g.`id` = r.`group_id`'
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

    def create_request(self, caller, items, sites, group, ncopies):
        now = int(time.time())

        if self.dry_run:
            return CopyRequest(0, caller.name, caller.dn, group, ncopies, 'new', now, now, 1)

        # Make an entry in registry
        columns = ('group', 'num_copies', 'user_id', 'first_request_time', 'last_request_time')
        values = (group, ncopies, caller.id, MySQL.bare('FROM_UNIXTIME(%d)' % now), MySQL.bare('FROM_UNIXTIME(%d)' % now))
        request_id = self.registry.insert_get_id('copy_requests', columns, values)

        fields = ('request_id', 'site')
        mapping = lambda site: (request_id, site)
        self.registry.insert_many('copy_request_sites', fields, mapping, sites)

        fields = ('request_id', 'item')
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('copy_request_items', fields, mapping, items)

        # Make an entry in history
        history_dataset_names = []
        history_block_names = []
        self.save_user(caller)
        self.save_items(items, history_dataset_names, history_block_names)
        self.save_sites(sites)
        self.save_group(group)

        sql = 'INSERT INTO `copy_requests` (`id`, `group_id`, `num_copies`, `user_id`, `request_time`)'
        sql += ' SELECT %s, g.`id`, %s, u.`id`, FROM_UNIXTIME(%s) FROM `groups` AS g, `users` AS u'
        sql += ' WHERE g.`name` = %s AND u.`dn` = %s'
        self.history.query(sql, request_id, ncopies, now, group, caller.dn)

        self.history.insert_select_many('copy_request_sites', ('request_id', 'site_id'), 'sites', (MySQL.bare('%d' % request_id), 'id'), 'name', sites)

        # history_dataset_names set in save_params
        self.history.insert_select_many('copy_request_datasets', ('request_id', 'dataset_id'), 'datasets', (MySQL.bare('%d' % request_id), 'id'), 'name', history_dataset_names)

        # history_block_names set in save_params
        self.history.insert_select_many('copy_request_blocks', ('request_id', 'block_id'), 'blocks', (MySQL.bare('%d' % request_id), 'id'), ('dataset_id', 'name'), history_block_names)

        return self.get_requests(request_id = request_id)[request_id]

    def update_request(self, request):
        if self.dry_run:
            return

        sql = 'UPDATE `copy_requests` SET `group` = %s, `num_copies` = %s, `last_request_time` = FROM_UNIXTIME(%s), `request_count` = %s WHERE `id` = %s'
        self.registry.query(sql, request.group, request.n, request.last_request, request.request_count, request.request_id)

        sql = 'UPDATE `copy_requests` SET `group_id` = (SELECT `id` FROM `groups` WHERE `name` = %s), `num_copies` = %s WHERE `id` = %s'
        self.history.query(sql, request.group, request.n, request.request_id)

    def cancel_request(self, request_id):
        if self.dry_run:
            return

        sql = 'UPDATE `copy_requests` SET `status` = \'cancelled\' WHERE `id` = %s'
        self.history.query(sql, request_id)

        sql = 'DELETE FROM r, a, i, s USING `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
        sql += ' INNER JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
        sql += ' INNER JOIN `copy_request_sites` AS s ON s.`request_id` = r.`id`'
        sql += ' WHERE r.`id` = %s'
        self.registry.query(sql, request_id)
