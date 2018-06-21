import time
import logging

from dynamo.request.common import RequestManager
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


class CopyRequestManager(RequestManager):
    def __init__(self, config = None):
        RequestManager.__init__(self, config, 'copy')

    def lock(self): #override
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('copy_requests', 'r'), 'copy_requests', ('active_copies', 'a'), 'active_copies',
            ('copy_request_items', 'i'), 'copy_request_items', ('copy_request_sites', 's'), 'copy_request_sites'
        ]

        if not self.dry_run:
            self.registry.lock_tables(write = tables)

    def get_requests(self, authorizer, request_id = None, statuses = None, users = None, items = None, sites = None):
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
            history_site_ids = self.history.save_sites(sites, get_ids = True)
        else:
            history_site_ids = None

        if items is not None:
            history_dataset_ids, history_block_ids = self.save_items(items)
        else:
            history_dataset_ids = None
            history_block_ids = None

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
            temp_table = self.make_temp_history_tables(history_dataset_ids, history_block_ids, history_site_ids)
            constraints.append('r.`id` IN (SELECT `id` FROM {0})'.format(temp_table))

        if len(constraints) != 0:
            sql += ' WHERE ' + ' AND '.join(constraints)

        sql += ' ORDER BY r.`id`'

        for rid, group, n, status, request_time, reason, user, dn in self.history.db.xquery(sql):
            if rid not in live_requests:
                archived_requests[rid] = CopyRequest(rid, user, dn, group, n, status, request_time, request_time, 1, reason)

        if len(archived_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, h.`name` FROM `copy_request_sites` AS s'
            sql += ' INNER JOIN `sites` AS h ON h.`id` = s.`site_id`'
            sql += ' WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, site in self.history.db.xquery(sql):
                archived_requests[rid].sites.append(site)
    
            # get the datasets
            sql = 'SELECT i.`request_id`, d.`name` FROM `copy_request_datasets` AS i'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = i.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset in self.history.db.xquery(sql):
                archived_requests[rid].items.append(dataset)

            # get the blocks
            sql = 'SELECT i.`request_id`, d.`name`, b.`name` FROM `copy_request_blocks` AS i'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = i.`block_id`'
            sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
            sql += ' WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in archived_requests.iterkeys())
            for rid, dataset, block in self.history.db.xquery(sql):
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

        mapping = lambda site: (request_id, site)
        self.registry.insert_many('copy_request_sites', ('request_id', 'site'), mapping, sites)
        mapping = lambda item: (request_id, item)
        self.registry.insert_many('copy_request_items', ('request_id', 'item'), mapping, items)

        # Make an entry in history
        history_user_ids = self.history.save_users([(caller.name, caller.dn)], get_ids = True)
        history_site_ids = self.history.save_sites(sites, get_ids = True)
        history_group_ids = self.history.save_groups([group], get_ids = True)
        history_dataset_ids, history_block_ids = self.save_items(items)

        sql = 'INSERT INTO `copy_requests` (`id`, `group_id`, `num_copies`, `user_id`, `request_time`)'
        sql += ' VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s))'
        self.history.db.query(sql, request_id, history_group_ids[0], ncopies, history_user_ids[0], now)

        mapping = lambda sid: (request_id, sid)
        self.history.db.insert_many('copy_request_sites', ('request_id', 'site_id'), mapping, history_site_ids)
        mapping = lambda did: (request_id, did)
        self.history.db.insert_select_many('copy_request_datasets', ('request_id', 'dataset_id'), mapping, history_dataset_ids)
        mapping = lambda bid: (request_id, bid)
        self.history.db.insert_select_many('copy_request_blocks', ('request_id', 'block_id'), mapping, history_block_ids)

        return self.get_requests(request_id = request_id)[request_id]

    def update_request(self, request):
        if self.dry_run:
            return

        sql = 'UPDATE `copy_requests` SET `status` = %s, `group_id` = (SELECT `id` FROM `groups` WHERE `name` = %s), `num_copies` = %s, `rejection_reason` = %s WHERE `id` = %s'
        self.history.db.query(sql, request.status, request.group, request.n, request.reject_reason, request.request_id)

        if request.status in ('new', 'activated'):
            sql = 'UPDATE `copy_requests` SET `status` = %s, `group` = %s, `num_copies` = %s, `last_request_time` = FROM_UNIXTIME(%s), `request_count` = %s WHERE `id` = %s'
            self.registry.query(sql, request.status, request.group, request.n, request.last_request, request.request_count, request.request_id)

        else:
            # terminal state
            sql = 'DELETE FROM r, a, i, s USING `copy_requests` AS r'
            sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
            sql += ' INNER JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
            sql += ' INNER JOIN `copy_request_sites` AS s ON s.`request_id` = r.`id`'
            sql += ' WHERE r.`id` = %s'
            self.registry.query(sql, request_id)

    def collect_updates(self, inventory):
        """
        Check active requests against the inventory state and set the status flags accordingly.
        """

        # Update active copy status
        sql_update = 'UPDATE `active_copies` SET `status` = \'completed\', `updated` = NOW() WHERE `request_id` = %s AND `item` = %s AND `site` = %s'

        if not self.dry_run:
            self.lock()

        try:
            sql = 'SELECT r.`id`, a.`item`, a.`site`, r.`group` FROM `active_copies` AS a'
            sql += ' INNER JOIN `copy_requests` AS r ON r.`id` = a.`request_id`'
            sql += ' WHERE a.`status` IN (\'new\', \'queued\')'
            
            for request_id, item_name, site_name, group_name in self.registry.query(sql):
                try:
                    site = inventory.sites[site_name]
                except KeyError:
                    LOG.error('Unknown site %s', site_name)
                    continue
                
                try:
                    group = inventory.groups[group_name]
                except KeyError:
                    LOG.error('Unknown group %s', group_name)
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
                        LOG.debug('Replica %s:%s not created yet', site.name, dataset.name)
                        continue
            
                    owners = set(br.group for br in replica.block_replicas)
            
                    if len(owners) > 1 or list(owners)[0] != group:
                        LOG.error('%s is not owned by %s.', replica, group)
                        continue
            
                    if replica.is_complete():
                        LOG.debug('%s complete', replica)
                        if not self.dry_run:
                            self.registry.query(sql_update, request_id, item_name, site_name)

                    else:
                        LOG.debug('%s incomplete', replica)
            
                else:
                    block = dataset.find_block(block_name)
                    if block is None:
                        LOG.error('Unknown block %s', item_name)
            
                    replica = site.find_block_replica(block)
                    if replica is None:
                        LOG.debug('Replica %s:%s not created yet', site.name, block.full_name())
                        continue
            
                    if replica.group != group:
                        LOG.error('%s is not owned by %s.', replica, group)
                        continue
            
                    if replica.is_complete():
                        LOG.debug('%s complete', replica)
                        if not self.dry_run:
                            self.registry.query(sql_update, request_id, item_name, site_name)
                    else:
                        LOG.debug('%s incomplete', replica)
                        
        finally:
            self.unlock()

        # Update request status

        if not self.dry_run:
            self.lock()

        try:
            active_requests = self.get_requests(statuses = ['activated'])

            for request in active_requests:
                if request.active_deletions is None:
                    LOG.error('No active copies for activated request %d', request.request_id)
                    continue

                n_complete = sum(1 for a in request.active_deletions if a[2] == 'completed')
                if n_complete == len(request.active_deletions):
                    request.status = 'completed'
                    self.update_request(request)

        finally:
            if not read_only:
                self.unlock()
