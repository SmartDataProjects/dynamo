import collections
import logging
import fnmatch

from dynamo.dealer.plugins.base import BaseHandler
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration, Dataset, Block, DatasetReplica, BlockReplica
from dynamo.dataformat.exceptions import ObjectError

LOG = logging.getLogger(__name__)

class CopyRequestsHandler(BaseHandler):
    """Process direct transfer requests made to the registry."""

    def __init__(self, config):
        BaseHandler.__init__(self, 'DirectRequests')

        db_config = Configuration(config.db_params)
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
        """

        partition = inventory.partitions[policy.partition_name]
        
        # full list of blocks to be proposed to Dealer
        blocks_to_propose = collections.defaultdict(lambda: collections.defaultdict(set)) # {site: {dataset: set of blocks}}

        # re-request all new active copies
        self.registry.query('LOCK TABLES `active_copies` WRITE')

        sql = 'SELECT `request_id`, `item`, `site` FROM `active_copies` WHERE `status` = \'new\''
        sql += ' ORDER BY `site`, `item`'

        fail_sql = 'UPDATE `active_copies` SET `status` = \'failed\' WHERE `request_id` = %s AND `item` = %s AND `site` = %s'

        active_requests = []

        _dataset_name = ''
        for request_id, item_name, site_name in self.registry.query(sql):
            try:
                site = inventory.sites[site_name]
            except KeyError:
                # shouldn't happen but for safety
                self.registry.query(fail_sql, request_id, item_name, site_name)
                continue

            try:
                dataset_name, block_name = Block.from_full_name(item_name)

            except ObjectError:
                # item_name is (supposed to be) a dataset name

                _dataset_name = ''

                try:
                    dataset = inventory.datasets[item_name]
                except KeyError:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                active_requests.append((dataset, site))

            else:
                # item_name is a block name

                if dataset_name != _dataset_name:
                    _dataset_name = dataset_name
                    active_requests.append(([], site))

                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                block = dataset.find_block(block_name)

                if block is None:
                    # shouldn't happen but for safety
                    self.registry.query(fail_sql, request_id, item_name, site_name)
                    continue

                active_requests[-1][0].append(block)

        for item, site in active_requests:
            # item is a dataset or a list of blocks
            if type(item) is Dataset:
                blocks_to_propose[site][item] = set(item.blocks)
            else:
                blocks_to_propose[site][item[0].dataset].update(item)

        self.registry.query('UNLOCK TABLES')

        # deal with new requests
        self.registry.query('LOCK TABLES `copy_requests` WRITE, `copy_requests` AS r WRITE, `copy_request_items` AS i WRITE, `active_copies` WRITE, `active_copies` AS a WRITE')

        sql = 'SELECT r.`id`, r.`site`, r.`group`, r.`num_copies`, i.`item`, r.`request_count`, r.`first_request_time` FROM `copy_requests` AS r'
        sql += ' INNER JOIN `copy_request_items` AS i ON i.`request_id` = r.`id`'
        sql += ' WHERE r.`status` = \'new\' OR r.`status` = \'updated\''

        # item can be the name of a dataset or a block
        # -> group into (site, group, # copies, request count, request time, [list of items], [list of active transfers])
        grouped_requests = {} # {request_id: copy info}

        for request_id, site_name, group_name, num_copies, item_name, request_count, request_time in self.registry.xquery(sql):
            if request_id not in grouped_requests:
                grouped_requests[request_id] = (site_name, group_name, num_copies, request_count, request_time, [], [])

            grouped_requests[request_id][5].append(item_name)

        sql = 'SELECT r.`id`, a.`item`, a.`site` FROM `copy_requests` AS r'
        sql += ' INNER JOIN `active_copies` AS a ON a.`request_id` = r.`id`'
        sql += ' WHERE r.`status` = \'new\' OR r.`status` = \'updated\'' # new requests shouldn't have any active copies, but just to be safe

        for request_id, item_name, site_name in self.registry.xquery(sql):
            grouped_requests[request_id][6].append((item_name, site_name))

        reject_sql = 'UPDATE `copy_requests` AS r SET r.`status` = \'rejected\', r.`rejection_reason` = %s'
        reject_sql += ' WHERE r.`id` = %s'

        def activate(request_id, item, site, status):
            activate_sql = 'INSERT INTO `active_copies` (`request_id`, `item`, `site`, `status`, `created`, `updated`)'
            activate_sql += ' VALUES (%s, %s, %s, %s, NOW(), NOW())'

            # item is a dataset or a list of blocks
            if type(item) is Dataset:
                self.registry.query(activate_sql, request_id, item.name, site.name, 'new')
            else:
                for block in item:
                    self.registry.query(activate_sql, request_id, block.full_name(), site.name, 'new')


        # loop over requests and find items and destinations
        for request_id, (site_name, group_name, num_copies, request_count, request_time, item_names, active_copies) in grouped_requests.iteritems():
            try:
                group = inventory.groups[group_name]
            except KeyError:
                self.registry.query(reject_sql, 'Invalid group name %s' % group_name, request_id)
                continue

            rejected = False
            items = [] # list of datasets or (list of blocks from a dataset)

            _dataset_name = ''
            # sorted(item_names) -> assuming dataset name comes first in the block full name so blocks get automatically clustered in the listing
            for item_name in sorted(item_names):
                try:
                    dataset_name, block_name = Block.from_full_name(item_name)

                except ObjectError:
                    # item_name is (supposed to be) a dataset name

                    _dataset_name = ''

                    # this is a dataset
                    try:
                        dataset = inventory.datasets[item_name]
                    except KeyError:
                        self.registry.query(reject_sql, 'Dataset %s not found' % item_name, request_id)
                        rejected = True
                        break

                    items.append(dataset)

                else:
                    # item_name is a block name

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

                    block = dataset.find_block(block_name)
                    if block is None:
                        self.registry.query(reject_sql, 'Block %s not found' % item_name, request_id)
                        rejected = True
                        break

                    # last element of the items list is a list
                    items[-1].append(block)

            if rejected:
                continue

            # elements of items are either a dataset or a list of blocks

            # process the items list
            for ii in range(len(items)):
                item = items[ii]
                if type(item) is Dataset:
                    if dataset.size > self.max_size:
                        self.registry.query(reject_sql, 'Dataset %s is too large (>%.0f TB)' % (dataset.name, self.max_size * 1.e-12), request_id)
                        rejected = True
                        break

                else:
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

            if rejected:
                continue

            new_requests = []
            wont_request = []

            # find destinations (num_copies times) for each item
            for item in items:
                # function to find existing copies
                # will not make a request only if there is a full copy of the item
                _, _, already_exists = policy.item_info(item)

                if '*' in site_name:
                    # count existing active copies
                    if type(item) is Dataset:
                        for it, st in active_copies:
                            if it == item.name:
                                num_copies -= 1
                    else:
                        # count the destinations of active copies
                        block_names = set(block.full_name() for block in item)
                        destinations = set()
                        for it, st in active_copies:
                            if it in block_names:
                                destinations.add(st)

                        num_copies -= len(destinations)

                    if num_copies <= 0:
                        continue

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

                    is_active = False

                    if type(item) is Dataset:
                        for it, st in active_copies:
                            if it == item.name:
                                is_active = True
                                break
                    else:
                        # count the destinations of active copies
                        block_names = set(block.full_name() for block in item)
                        for it, st in active_copies:
                            if it in block_names:
                                is_active = True
                                break

                    if is_active:
                        continue

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
                activate(request_id, item, site, 'new')

                if type(item) is Dataset:
                    blocks_to_propose[site][item] = set(item.blocks)
                else:
                    blocks_to_propose[site][item[0].dataset].update(item)

            for item, site in wont_request:
                activate(request_id, item, site, 'completed')

            self.registry.query('UPDATE `copy_requests` SET `status` = \'activated\' WHERE `id` = %s', request_id)

        self.registry.query('UNLOCK TABLES')

        # form the final proposal
        dealer_requests = []
        for site, block_list in blocks_to_propose.iteritems():
            for dataset, blocks in block_list.iteritems():
                if blocks == dataset.blocks:
                    dealer_requests.append((dataset, site))
                else:
                    daeler_requests.append((list(blocks), site))

        return dealer_requests

    def postprocess(self, run_number, history, copy_list): # override
        """
        Create active copy entries for accepted copies.
        """

        sql = 'UPDATE `active_copies` SET `status` = \'queued\', `updated` = NOW() WHERE `item` LIKE %s AND `site` = %s AND `status` = \'new\''

        for replica in copy_list:
            if type(replica) is DatasetReplica:
                # active copies with dataset name
                self.registry.query(sql, replica.dataset.name, replica.site.name)

                # active copies with block name
                self.registry.query(sql, Block.to_full_name(replica.dataset.name, '%'), replica.site.name)

            else:
                self.registry.query(sql, replica.block.full_name(), replica.site.name)
