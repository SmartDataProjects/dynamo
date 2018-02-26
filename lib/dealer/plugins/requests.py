import logging

from dynamo.dealer.plugins.base import BaseHandler
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration, Dataset, Block, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class CopyRequestsHandler(BaseHandler):
    """Process direct transfer requests made to the registry."""

    def __init__(self, config):
        BaseHandler.__init__(self, 'DirectRequests')

        db_config = Configuration(config.db_config)
        db_config['reuse_connection'] = True # need to work with table locks
        
        self.registry = MySQL(db_config)

        # maximum size that can be requested
        self.max_size = config.max_size * 1.e+12

        # convert block-level requests to dataset-level if requested size is greater than
        # dataset size * block_request_max
        self.block_request_max = config.block_request_max

    def get_requests(self, inventory, history, policy): # override
        """
        1. Request all active transfers in new state (these were not queued in the last cycle)
        2. Find all transfer requests with status new or updated.
        3. Decide whether to accept the request. Set status accordingly.
        4. Find the destinations if wildcard was used.
        In multiple parts of this module, block name is assumed to have the form dataset#block
        """

        partition = inventory.partitions[policy.partition_name]
        
        # final list to be returned
        dealer_requests = [] # [(item, site)]

        # re-request all new active copies
        self.registry.query('LOCK TABLES `active_copies` AS a WRITE')

        sql = 'SELECT a.`request_id`, a.`item`, a.`site`, FROM `active_copies` AS a WHERE a.`status` = \'new\''
        sql += ' ORDER BY a.`site`, a.`item`'

        fail_sql = 'UPDATE `active_copies` AS a SET a.`status` = \'failed\' WHERE a.`request_id` = %s AND a.`item` = %s AND a.`site` = %s'

        active_requests = []

        _dataset_name = ''
        for request_id, item_name, site_name in self.registry.query(sql):
            try:
                site = inventory.sites[site_name]
            except KeyError:
                # shouldn't happen but for safety
                self.registry.query(fail_sql, request_id, item_name, site_name)
                continue

            if '#' in item_name:
                dataset_name = item_name[:item_name.find('#')]
                if dataset_name != _dataset_name:
                    _dataset_name = dataset_name
                    active_requests.append(([], site))

                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                block_name = item_name[item_name.find('#') + 1:]
                block = dataset.find_block(Block.to_internal_name(block_name))

                if block is None:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                active_requests[-1][0].append(block)

            else:
                _dataset_name = ''

                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                active_requests.append((dataset, site))

        for item, site in active_requests:
            if type(item) is list and len(item) == 1:
                # convert to single block request
                dealer_requests.append((item[0], site))
            else:
                dealer_requests.append((item, site))

        self.registry.query('UNLOCK TABLES')

        # deal with new requests
        self.registry.query('LOCK TABLES `copy_requests` AS r WRITE, `copy_request_items` AS i WRITE, `active_copies` AS a WRITE')

        sql = 'SELECT r.`id`, r.`site`, r.`group`, r.`num_copies`, i.`item` FROM `copy_requests` AS r'
        sql += ' INNER JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
        sql += ' WHERE r.`status` = \'new\' OR r.`status` = \'updated\''
        sql += ' ORDER BY r.`request_count`, r.`first_request_time`'

        # item can be the name of a dataset or a block
        # -> group into (request id, site, group, # copies, [list of items])
        grouped = []

        _request_id = 0
        for request_id, site_name, group_name, num_copies, item_name in self.registry.xquery(sql):
            if request_id != _request_id:
                _request_id = request_id
                grouped.append((request_id, site_name, group_name, num_copies, []))

            grouped[-1][4].append(item_name)

        reject_sql = 'UPDATE `copy_requests` AS r SET r.`status` = \'rejected\', r.`rejection_reason` = %s'
        reject_sql += ' WHERE r.`request_id` = %s'

        def activate(request_id, item, site, status):
            activate_sql = 'INSERT INTO `active_copies` (`request_id`, `item`, `site`, `status`, `created`, `updated`)'
            activate_sql += ' VALUES (%s, %s, %s, %s, %s, NOW(), NOW())'

            if type(item) is Dataset:
                self.registry.query(activate_sql, request_id, item.name, site.name, 'new')
            elif type(item) is Block:
                self.registry.query(activate_sql, request_id, item.full_name(), site.name, 'new')
            else:
                for block in item:
                    self.registry.query(activate_sql, request_id, block.full_name(), site.name, 'new')


        # loop over requests and find items and destinations
        for request_id, site_name, group_name, num_copies, item_names in grouped:
            try:
                group = inventory.groups[group_name]
            except KeyError:
                self.registry.query(reject_sql, 'Invalid group name %s' % group_name, request_id)
                continue

            rejected = False
            items = [] # list of datasets or (list of blocks from a dataset)

            _dataset_name = ''
            for item_name in sorted(item_names):
                if '#' in item_name:
                    # this is a block
                    dataset_name = item_name[:item_name.find('#')]

                    if dataset_name != _dataset_name:
                        # of a new dataset
                        try:
                            dataset = inventory.datasets[dataset_name]
                        except KeyError:
                            # if any of the dataset name is invalid, reject the entire request
                            self.registry.query(reject_sql, 'Dataset %s not found' % dataset_name, request_id)
                            rejected = True
                            break

                        _dataset_name = dataset_name
                        items.append([])
    
                    block_name = item_name[item_name.find('#') + 1:]
                    block = dataset.find_block(Block.to_internal_name(block_name))
                    if block is None:
                        self.registry.query(reject_sql, 'Block %s not found' % item_name, request_id)
                        rejected = True
                        break

                    # last element of the items list is a list
                    items[-1].append(block)

                else:
                    _dataset_name = ''

                    # this is a dataset
                    try:
                        dataset = inventory.datasets[item_name]
                    except KeyError:
                        self.registry.query(reject_sql, 'Dataset %s not found' % item_name, request_id)
                        rejected = True
                        break

                    items.append(dataset)

            if rejected:
                continue

            # process the items list
            for ii in range(len(items)):
                item = items[ii]
                if type(item) is list:
                    dataset = item[0].dataset

                    total_size = sum(b.size for b in item)

                    if total_size > self.max_size:
                        self.registry.query(reject_sql, 'Request size for %s too large (>%.0f TB)' % (dataset.name, self.max_size * 1.e-12), request_id)
                        rejected = True
                        break

                    if total_size > float(dataset.size) * self.block_request_max:
                        # if the total size of requested blocks is large enough, just copy the dataset
                        # covers the case where we actually have the full list of blocks (if block_request_max is less than 1)
                        items[ii] = dataset

                    elif len(item) == 1:
                        # a single block request
                        items[ii] = item[0]

                else:
                    if dataset.size > self.max_size:
                        self.registry.query(reject_sql, 'Dataset %s is too large (>%.0f TB)' % (dataset.name, self.max_size * 1.e-12), request_id)
                        rejected = True
                        break

            if rejected:
                continue

            new_requests = []
            wont_request = []

            # find destinations (num_copies times) for each item
            for item in items:
                # function to find existing copies
                _, _, already_exists = policy.item_info(item)

                if '*' in site_name:
                    # check the existing copies and create activation entries with status completed
                    for site in inventory.sites.itervalues():
                        if not fnmatch.fnmatch(site.name, site_name):
                            continue

                        if already_exists(site, item):
                            wont_request.append((item, site))
                            num_copies -= 1

                        if num_copies == 0:
                            break

                    matched_destinations = []
                    for icopy in range(num_copies):
                        destination, item_name, _, _ = policy.find_destination_for(item, partition, match_patterns = [site_name], exclude_patterns = matched_destinations)
    
                        if destination is None:
                            # if any of the item cannot find any of the num_copies destinations, reject the request
                            self.registry.query(reject_sql, 'Destination %d for %s not available' % (icopy, item_name), request_id)
                            rejected = True
                            break
    
                        matched_destinations.append(destination.name)
                        new_requests.append((item, destination))

                else:
                    # if a destination is specified, num_copies must be 1
                    try:
                        destination = inventory.sites[site_name]
                    except KeyError:
                        self.registry.query(reject_sql, 'Invalid site name %s' % site_name, request_id)
                        rejected = True
                        break

                    if already_exists(destination, item):
                        wont_request.append((item, destination))
                    else:
                        item_name, _, rejection_reason = policy.check_destination(item, destination, partition)
                        
                        if rejection_reason is not None:
                            # item_name is guaranteed to be valid
                            self.registry.query(reject_sql, 'Cannot copy %s to %s' % (item_name, site_name), request_id)
                            rejected = True
    
                        new_requests.append((item, destination))

                if rejected:
                    break

            if rejected:
                continue

            # finally add to the returned requests
            for item, site in new_requests:
                dealer_requests.append((item, site))
                activate(request_id, item, site, 'new')

            for item, site in wont_request:
                activate(request_id, item, site, 'completed')

            self.registry.query('UPDATE `copy_requests` SET `status` = \'activated\' WHERE `id` = %s', request_id)

        self.registry.query('UNLOCK TABLES')

        return dealer_requests

    def postprocess(self, run_number, history, copy_list): # override
        """
        Create active copy entries for accepted copies.
        """

        sql = 'UPDATE `active_copies` SET `status` = \'queued\', `updated` = NOW() WHERE `item` = %s AND `site` = %s AND `status` = \'new\''

        for replica in copy_list:
            if type(replica) is DatasetReplica:
                if len(replica.block_replicas) == len(replica.dataset.blocks):
                    # dataset-level request
                    self.registry.query(sql, replica.dataset.name, replica.site.name)
                else:
                    for block_replica in replica.block_replicas:
                        self.registry.query(sql, block_replica.block.full_name(), replica.site.name)

            else:
                self.registry.query(sql, replica.block.full_name(), replica.site.name)
