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

    def lock(self): #override
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('deletion_requests', 'r'), 'deletion_requests', ('active_deletions', 'a'), 'active_deletions',
            ('deletion_request_items', 'i'), 'deletion_request_items', ('deletion_request_sites', 's'), 'deletion_request_sites'
        ]

        if not self.dry_run:
            self.registry.lock_tables(write = tables)

    def get_requests(self, authorizer, request_id = None, statuses = None, users = None, items = None, sites = None):
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
            history_site_ids = self.history.save_sites(sites, get_ids = True)
        else:
            history_site_ids = None

        if items is not None:
            history_dataset_ids, history_block_ids = self.save_items(items)
        else:
            history_dataset_ids = None
            history_block_ids = None

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
            temp_table = self.make_temp_history_tables(history_dataset_ids, history_block_ids, history_site_ids)
            constraints.append('r.`id` IN (SELECT `id` FROM {0})'.format(temp_table))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        for rid, status, request_time, reason, user, dn in self.history.db.xquery(sql):
            if rid not in live_requests:
                archived_requests[rid] = DeletionRequest(rid, user, dn, status, request_time, reason)

        if len(archived_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, h.`name` FROM `deletion_request_sites` AS s'
            sql += ' INNER JOIN `sites` AS h ON h.`id` = s.`site_id`'
            sql += ' WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, site in self.history.db.xquery(sql):
                archived_requests[rid].sites.append(site)
    
            # get the datasets
            sql = 'SELECT i.`request_id`, d.`name` FROM `deletion_request_datasets` AS i'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = i.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset in self.history.db.xquery(sql):
                archived_requests[rid].items.append(dataset)

            # get the blocks
            sql = 'SELECT i.`request_id`, d.`name`, b.`name` FROM `deletion_request_blocks` AS i'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = i.`block_id`'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset, block in self.history.db.xquery(sql):
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

        mapping = lambda site: (request_id, site)
        self.registry.insert_many('deletion_request_sites', ('request_id', 'site'), mapping, sites)
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('deletion_request_items', ('request_id', 'item'), mapping, items)

        # Make an entry in history
        history_user_ids = self.history.save_users([(caller.name, caller.dn)], get_ids = True)
        history_site_ids = self.history.save_sites(sites, get_ids = True)
        history_dataset_ids, history_block_ids = self.save_items(items)

        sql = 'INSERT INTO `deletion_requests` (`id`, `user_id`, `request_time`)'
        sql += ' SELECT %s, u.`id`, FROM_UNIXTIME(%s) FROM `groups` AS g, `users` AS u'
        sql += ' WHERE u.`dn` = %s'
        self.history.db.query(sql, request_id, now, caller.dn)

        mapping = lambda sid: (request_id, sid)
        self.history.db.insert_many('deletion_request_sites', ('request_id', 'site_id'), mapping, history_site_ids)
        mapping = lambda did: (request_id, did)
        self.history.db.insert_select_many('deletion_request_datasets', ('request_id', 'dataset_id'), mapping, history_dataset_ids)
        mapping = lambda bid: (request_id, bid)
        self.history.db.insert_select_many('deletion_request_blocks', ('request_id', 'block_id'), mapping, history_block_ids)

        return self.get_requests(request_id = request_id)[request_id]

    def update_request(self, request):
        if self.dry_run:
            return

        sql = 'UPDATE `deletion_requests` SET `status` = %s, `rejection_reason` = %s WHERE `id` = %s'
        self.history.db.query(sql, request.status, request.reject_reason, request.request_id)

        if request.status in ('new', 'activated'):
            sql = 'UPDATE `deletion_requests` SET `status` = %s, `request_time` = FROM_UNIXTIME(%s) WHERE `id` = %s'
            self.registry.query(sql, request.status, request.request_time, request.request_id)

        else:
            # terminal state
            sql = 'DELETE FROM r, a, i, s USING `deletion_requests` AS r'
            sql += ' INNER JOIN `active_deletions` AS a ON a.`request_id` = r.`id`'
            sql += ' INNER JOIN `deletion_request_items` AS i ON i.`request_id` = r.`id`'
            sql += ' INNER JOIN `deletion_request_sites` AS s ON s.`request_id` = r.`id`'
            sql += ' WHERE r.`id` = %s'
            self.registry.query(sql, request_id)

    def collect_updates(self, inventory):
        """
        Check active requests against the inventory state and set the status flags accordingly.
        """

        # Update active deletion status
        sql_update = 'UPDATE `active_deletions` SET `status` = \'completed\', `updated` = NOW() WHERE `request_id` = %s AND `item` = %s AND `site` = %s'

        if not self.dry_run:
            self.lock()

        try:
            sql = 'SELECT r.`id`, a.`item`, a.`site` FROM `active_deletions` AS a'
            sql += ' INNER JOIN `deletion_requests` AS r ON r.`id` = a.`request_id`'
            sql += ' WHERE a.`status` IN (\'new\', \'queued\')'
            
            for request_id, item_name, site_name in self.registry.query(sql):
                try:
                    site = inventory.sites[site_name]
                except KeyError:
                    LOG.error('Unknown site %s', site_name)
                    continue
                
                try:
                    dataset_name, block_name = df.Block.from_full_name(item_name)
                except df.ObjectError:
                    dataset_name = item_name
                    block_name = None
                else:
                    pass
                    
                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    LOG.error('Unknown dataset %s', dataset_name)
                    continue
            
                if block_name is None:
                    replica = site.find_dataset_replica(dataset)
                    if replica is None:
                        LOG.debug('%s gone', replica)
                        if not self.dry_run:
                            self.registry.query(sql_update, request_id, item_name, site_name)
            
                else:
                    block = dataset.find_block(block_name)
                    if block is None:
                        LOG.error('Unknown block %s', item_name)
            
                    replica = site.find_block_replica(block)
                    if replica is None:
                        LOG.debug('Replica %s:%s gone', site.name, block.full_name())
                        if not self.dry_run:
                            self.registry.query(sql_update, request_id, item_name, site_name)
                        
        finally:
            if not self.dry_run:
                self.unlock()

        # Update request status

        if not self.dry_run:
            self.lock()

        try:
            active_requests = self.get_requests(statuses = ['activated'])

            for request in active_requests:
                if request.active_deletions is None:
                    LOG.error('No active deletions for activated request %d', request.request_id)
                    continue

                n_complete = sum(1 for a in request.active_deletions if a[2] == 'completed')
                if n_complete == len(request.active_deletions):
                    request.status = 'completed'
                    self.update_request(request)
        
        if not read_only:
            self.unlock()
