import time
import logging

from dynamo.request.common import RequestManager
from dynamo.utils.interface.mysql import MySQL
import dynamo.dataformat as df
from dynamo.dataformat.request import Request, RequestAction, CopyRequest

LOG = logging.getLogger(__name__)

class CopyRequestManager(RequestManager):
    def __init__(self, config = None):
        RequestManager.__init__(self, 'copy', config)
        LOG.info("Initializing CopyRequestManager with config:")
        LOG.info(config)

    def lock(self): #override
        # Caller of this function is responsible for unlocking
        # Non-aliased locks are for insert & update statements later
        tables = [
            ('copy_requests', 'r'), 'copy_requests', ('active_copies', 'a'), 'active_copies',
            ('copy_request_items', 'i'), 'copy_request_items', ('copy_request_sites', 's'), 'copy_request_sites',
            ('cached_copy_requests','c'), 'cached_copy_requests'
        ]

        if not self._read_only:
            self.registry.db.lock_tables(write = tables)

    def get_requests(self, request_id = None, statuses = None, users = None, items = None, sites = None):
        all_requests = {}

        sql = 'SELECT r.`id`, r.`group`, r.`num_copies`, 0+r.`status`, UNIX_TIMESTAMP(r.`first_request_time`), UNIX_TIMESTAMP(r.`last_request_time`),'
        sql += ' r.`request_count`, r.`user`, r.`dn`, a.`item`, a.`site`, 0+a.`status`, UNIX_TIMESTAMP(a.`updated`)'
        sql += ' FROM `copy_requests` AS r'
        sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
        sql += self._make_registry_constraints(request_id, statuses, users, items, sites)
        sql += ' ORDER BY r.`id`'

        _rid = 0

        for rid, group, n, status, first_request, last_request, count, user, dn, a_item, a_site, a_status, a_update in self.registry.db.xquery(sql):
            if rid != _rid:
                _rid = rid
                request = all_requests[rid] = CopyRequest(rid, user, dn, group, n, int(status), first_request, last_request, count)

                if request.status == CopyRequest.ST_ACTIVATED:
                    request.actions = []

            if a_item is not None:
                request.actions.append(RequestAction(a_item, a_site, int(a_status), a_update))

        if len(all_requests) != 0:
            # get the sites
            sql = 'SELECT s.`request_id`, s.`site` FROM `copy_request_sites` AS s WHERE s.`request_id` IN (%s)' %  ','.join('%d' % d for d in all_requests.iterkeys())
            for rid, site in self.registry.db.xquery(sql):
                all_requests[rid].sites.append(site)
    
            # get the items
            sql = 'SELECT i.`request_id`, i.`item` FROM `copy_request_items` AS i WHERE i.`request_id` IN (%s)' %  ','.join('%d' % d for d in all_requests.iterkeys())
            for rid, item in self.registry.db.xquery(sql):
                all_requests[rid].items.append(item)

        if items is not None or sites is not None:
            self.registry.db.drop_tmp_table('ids_tmp')

        if (request_id is not None and len(all_requests) != 0) or \
           (statuses is not None and (set(statuses) <= set(['new', 'activated']) or set(statuses) <= set([Request.ST_NEW, Request.ST_ACTIVATED]))):
            # there's nothing in the archive
            return all_requests

        # Pick up archived requests from the history DB
        archived_requests = {}

        sql = 'SELECT r.`id`, g.`name`, r.`num_copies`, 0+r.`status`, UNIX_TIMESTAMP(r.`request_time`),'
        sql += ' r.`rejection_reason`, u.`name`, u.`dn`'
        sql += ' FROM `copy_requests` AS r'
        sql += ' INNER JOIN `groups` AS g ON g.`id` = r.`group_id`'
        sql += ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`'
        sql += self._make_history_constraints(request_id, statuses, users, items, sites)
        sql += ' ORDER BY r.`id`'

        for rid, group, n, status, request_time, reason, user, dn in self.history.db.xquery(sql):
            if rid not in all_requests:
                archived_requests[rid] = CopyRequest(rid, user, dn, group, n, int(status), request_time, request_time, 1, reason)

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

        all_requests.update(archived_requests)

        if items is not None or sites is not None:
            self.history.db.drop_tmp_table('ids_tmp')

        return all_requests

    def create_request(self, caller, items, sites, sites_original, group, ncopies):
        now = int(time.time())

        if self._read_only:
            return CopyRequest(0, caller.name, caller.dn, group, ncopies, 'new', now, now, 1)

        # Make an entry in registry
        columns = ('group', 'num_copies', 'user', 'dn', 'first_request_time', 'last_request_time')
        values = (group, ncopies, caller.name, caller.dn, MySQL.bare('FROM_UNIXTIME(%d)' % now), MySQL.bare('FROM_UNIXTIME(%d)' % now))
        LOG.info(values)
        request_id = self.registry.db.insert_get_id('copy_requests', columns, values)

        mapping = lambda site: (request_id, site)
        self.registry.db.insert_many('copy_request_sites', ('request_id', 'site'), mapping, sites)
        mapping = lambda item: (request_id, item)

        self.registry.db.insert_many('copy_request_items', ('request_id', 'item'), mapping, items)

        # Make an entry in history
        history_user_ids = self.history.save_users([(caller.name, caller.dn)], get_ids = True)
        history_site_ids = self.history.save_sites(sites_original, get_ids = True)
        history_group_ids = self.history.save_groups([group], get_ids = True)
        history_dataset_ids, history_block_ids = self._save_items(items)

        sql = 'INSERT INTO `copy_requests` (`id`, `group_id`, `num_copies`, `user_id`, `request_time`)'
        sql += ' VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s))'
        self.history.db.query(sql, request_id, history_group_ids[0], ncopies, history_user_ids[0], now)

        mapping = lambda sid: (request_id, sid)
        self.history.db.insert_many('copy_request_sites', ('request_id', 'site_id'), mapping, history_site_ids)
        mapping = lambda did: (request_id, did)
        self.history.db.insert_many('copy_request_datasets', ('request_id', 'dataset_id'), mapping, history_dataset_ids)
        mapping = lambda bid: (request_id, bid)
        self.history.db.insert_many('copy_request_blocks', ('request_id', 'block_id'), mapping, history_block_ids)

        return self.get_requests(request_id = request_id)[request_id]

    def create_cached_request(self, caller, item, sites_original, group, ncopies):
        now = int(time.time())

        # Make an entry in registry
        columns = ('item', 'sites', 'group', 'num_copies', 'user', 'dn', 'request_time', 'status')
        values = (item, sites_original, group, ncopies, caller.name, caller.dn, MySQL.bare('FROM_UNIXTIME(%d)' % now), 'new')
        LOG.info(values)
        cached_request_id = self.registry.db.insert_get_id('cached_copy_requests', columns, values)

        return_dict = {}
        return_dict['request_id'] = cached_request_id
        return_dict['item'] = item
        return_dict['sites'] = sites_original

        return return_dict

    def update_request(self, request):
        if self._read_only:
            return

        sql = 'UPDATE `copy_requests` SET `status` = %s, `group_id` = (SELECT `id` FROM `groups` WHERE `name` = %s), `num_copies` = %s, `rejection_reason` = %s WHERE `id` = %s'
        self.history.db.query(sql, request.status, request.group, request.n, request.reject_reason, request.request_id)

        if request.status in (Request.ST_NEW, Request.ST_ACTIVATED):
            sql = 'UPDATE `copy_requests` SET `status` = %s, `group` = %s, `num_copies` = %s, `last_request_time` = FROM_UNIXTIME(%s), `request_count` = %s WHERE `id` = %s'
            self.registry.db.query(sql, request.status, request.group, request.n, request.last_request, request.request_count, request.request_id)

            if request.actions is not None:
                # insert or update active copies
                fields = ('request_id', 'item', 'site', 'status', 'created', 'updated')
                update_columns = ('status', 'updated')
                for a in request.actions:
                    now = time.strftime('%Y-%m-%d %H:%M:%S') # current local time
                    values = (request.request_id, a.item, a.site, a.status, now, now)
                    self.registry.db.insert_update('active_copies', fields, *values, update_columns = update_columns)

        else:
            # terminal state
            sql = 'DELETE FROM r, a, i, s USING `copy_requests` AS r'
            sql += ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
            sql += ' LEFT JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
            sql += ' LEFT JOIN `copy_request_sites` AS s ON s.`request_id` = r.`id`'
            sql += ' WHERE r.`id` = %s'
            self.registry.db.query(sql, request.request_id)        

    def collect_updates(self, inventory):
        """
        Check active requests against the inventory state and set the status flags accordingly.
        """

        now = int(time.time())

        incomplete_replicas = ([], [])

        self.lock()

        try:
            active_requests = self.get_requests(statuses = [Request.ST_ACTIVATED])

            for request in active_requests.itervalues():
                if request.actions is None:
                    LOG.error('No active copies for activated request %d', request.request_id)
                    request.status = Request.ST_COMPLETED
                    self.update_request(request)

                    continue

                try:
                    group = inventory.groups[request.group]
                except KeyError:
                    LOG.error('Unknown group %s', request.group)
                    request.status = Request.REJECTED
                    request.reject_reason = 'Unknown group %s' % request.group
                    self.update_request(request)

                    continue

                updated = False

                for action in request.actions:
                    if action.status != RequestAction.ST_QUEUED:
                        continue

                    try:
                        site = inventory.sites[action.site]
                    except KeyError:
                        LOG.error('Unknown site %s', action.site)
                        action.status = RequestAction.ST_FAILED
                        action.last_update = now
                        updated = True

                        continue
               
                    try:
                        dataset_name, block_name = df.Block.from_full_name(action.item)
                    except df.ObjectError:
                        dataset_name = action.item
                        block_name = None
                    else:
                        pass
                        
                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        LOG.error('Unknown dataset %s', dataset_name)
                        action.status = RequestAction.ST_FAILED
                        action.last_update = now
                        updated = True

                        continue

                    if block_name is None:
                        # looking for a dataset replica

                        replica = site.find_dataset_replica(dataset)
                        if replica is None:
                            if action.last_update > now - 1800: # 30 minutes grace period to avoid race condition with dealer
                                continue

                            LOG.info('Replica %s:%s disappeared. Resetting the status to new.', site.name, dataset.name)
                            action.status = RequestAction.ST_NEW
                            action.last_update = now
                            updated = True

                        else:
                            if not replica.growing or replica.group is not group:
                                LOG.error('%s is not a growing replica owned by %s. Resetting action status to new.', replica, group)
                                action.status = RequestAction.ST_NEW
                                action.last_update = now
                                updated = True
                            elif replica.is_complete():
                                LOG.debug('%s complete', replica)
                                action.status = RequestAction.ST_COMPLETED
                                action.last_update = now
                                updated = True
                            else:
                                incomplete_replicas[0].append(replica)
                                LOG.debug('%s incomplete', replica)
                
                    else:
                        block = dataset.find_block(block_name)
                        if block is None:
                            LOG.error('Unknown block %s', action.item)
                            action.status = RequestAction.ST_FAILED
                            action.last_update = now
                            updated = True

                            continue
                
                        replica = site.find_block_replica(block)
                        if replica is None:
                            if action.last_update > now - 1800: # 30 minutes grace period to avoid race condition with dealer
                                continue

                            LOG.info('Replica %s:%s disappeared. Resetting the status to new.', site.name, block.full_name())
                            action.status = RequestAction.ST_NEW
                            action.last_update = now
                            updated = True

                        else:
                            if replica.group != group:
                                LOG.error('%s is not owned by %s.', replica, group)
                                action.status = RequestAction.ST_NEW
                                action.last_update = now
                                updated = True
                            elif replica.is_complete():
                                LOG.debug('%s complete', replica)
                                action.status = RequestAction.ST_COMPLETED
                                action.last_update = now
                                updated = True
                            else:
                                incomplete_replicas[1].append(replica)
                                LOG.debug('%s incomplete', replica)

                n_complete = sum(1 for a in request.actions if a.status in (RequestAction.ST_COMPLETED, RequestAction.ST_FAILED))
                if n_complete == len(request.actions):
                    request.status = Request.ST_COMPLETED
                    updated = True

                if updated:
                    self.update_request(request)

        finally:
            self.unlock()

        return incomplete_replicas
