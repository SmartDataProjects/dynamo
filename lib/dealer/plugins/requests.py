import logging
import time

from dynamo.dealer.plugins.base import BaseHandler, DealerRequest
from dynamo.request.copy import CopyRequestManager
from dynamo.dataformat import Configuration, Dataset, Block
from dynamo.dataformat.request import Request, RequestAction
from dynamo.dataformat.exceptions import ObjectError

LOG = logging.getLogger(__name__)

class CopyRequestsHandler(BaseHandler):
    """Process direct transfer requests made to the registry."""

    def __init__(self, config):
        BaseHandler.__init__(self, 'DirectRequests')

        registry_config = Configuration(config.registry)
        registry_config['reuse_connection'] = True # need to work with table locks

        self.request_manager = CopyRequestManager(config.get('manager', None))

        # maximum size that can be requested
        self.max_size = config.max_size * 1.e+12

        # convert block-level requests to dataset-level if requested size is greater than
        # dataset size * block_request_max
        self.block_request_max = config.block_request_max

        # list of group names from which ownership of blocks can be taken away
        self.overwritten_groups = config.get('overwritten_groups', [])

        self.activated_requests = []

    def set_read_only(self, value = True): #override
        self._read_only = value
        self.request_manager.set_read_only(value)

    def get_requests(self, inventory, policy): #override
        """
        1. Request all active transfers in new state (these were not queued in the last cycle)
        2. Find all transfer requests with status new.
        3. Decide whether to accept the request. Set status accordingly.
        4. Find the destinations if wildcard was used.
        """

        partition = inventory.partitions[policy.partition_name]

        overwritten_groups = [inventory.groups[name] for name in self.overwritten_groups]

        self.activated_requests = []
        
        # full list of blocks to be proposed to Dealer
        blocks_to_propose = {} # {site: {dataset: set of blocks}}

        now = int(time.time())

        # Re-request new actions within activated requests

        self.request_manager.lock()
        active_requests = self.request_manager.get_requests(statuses = [Request.ST_ACTIVATED])

        activation_list = []

        for request in active_requests.itervalues():
            updated = False
            to_be_activated = False

            for action in request.actions:
                if action.status != RequestAction.ST_NEW:
                    continue

                try:
                    site = inventory.sites[action.site]
                except KeyError:
                    action.status = RequestAction.ST_FAILED
                    action.last_update = now
                    updated = True
                    continue
    
                try:
                    dataset_name, block_name = Block.from_full_name(action.item)
    
                except ObjectError:
                    # action.item is (supposed to be) a dataset name

                    try:
                        dataset = inventory.datasets[action.item]
                    except KeyError:
                        action.status = RequestAction.ST_FAILED
                        action.last_update = now
                        updated = True                        
                        continue

                    existing_replica = site.find_dataset_replica(dataset)

                    if existing_replica is not None:
                        if existing_replica.is_complete():
                            action.status = RequestAction.ST_COMPLETED
                        else:
                            # it was queued by someone
                            action.status = RequestAction.ST_QUEUED
                        action.last_update = now
                        updated = True

                    else:
                        activation_list.append((dataset, site))
                        to_be_activated = True
    
                else:
                    # action.item is a block name
    
                    try:
                        dataset = inventory.datasets[dataset_name]
                    except KeyError:
                        action.status = RequestAction.ST_FAILED
                        action.last_update = now
                        updated = True                        
                        continue
    
                    block = dataset.find_block(block_name)
    
                    if block is None:
                        action.status = RequestAction.ST_FAILED
                        action.last_update = now
                        updated = True                        
                        continue

                    existing_replica = block.find_replica(site)

                    if existing_replica is not None:
                        if existing_replica.is_complete():
                            action.status = RequestAction.ST_COMPLETED
                        else:
                            action.status = RequestAction.ST_QUEUED
                        action.last_update = now
                        updated = True

                    else:
                        activation_list.append((block, site))
                        to_be_activated = True

            if updated:
                self.request_manager.update_request(request)

            if to_be_activated:
                self.activated_requests.append(request)

        self.request_manager.unlock()

        for item, site in activation_list:
            try:
                site_blocks = blocks_to_propose[site]
            except KeyError:
                site_blocks = blocks_to_propose[site] = {}

            if type(item) is Dataset:
                site_blocks[item] = set(item.blocks)
            else:
                dataset = item.dataset
                try:
                    blocks = site_blocks[dataset]
                except KeyError:
                    blocks = site_blocks[dataset] = set()

                blocks.add(item)

        ## deal with new requests
        self.request_manager.lock()
        new_requests = self.request_manager.get_requests(statuses = [Request.ST_NEW])

        def reject(request, reason):
            request.status = Request.ST_REJECTED
            request.reject_reason = reason
            self.request_manager.update_request(request)

        for request in new_requests.itervalues():
            try:
                group = inventory.groups[request.group]
            except KeyError:
                reject(request, 'Invalid group name %s' % request.group)
                continue

            invalid_items = []
            datasets = request.find_items(inventory, invalid_items)
            sites = filter(lambda s: s in policy.target_sites, request.find_sites(inventory))

            if len(invalid_items) != 0:
                reject(request, 'Invalid item names: [%s]' % ','.join(invalid_items))
                continue

            if len(sites) == 0:
                reject(request, 'Target sites not available for transfers')
                continue

            # convert to DealerRequests
            proto_dealer_requests = []

            # process the items list
            for dataset, blocks in datasets.iteritems():
                if blocks is None:
                    if dataset.size > self.max_size:
                        reject(request, 'Dataset %s is too large (>%.0f TB)' % (dataset.name, self.max_size * 1.e-12))
                        break

                    item = dataset

                else:
                    total_size = sum(b.size for b in blocks)

                    if total_size > self.max_size:
                        reject(request, 'Request size for %s too large (>%.0f TB)' % (dataset.name, self.max_size * 1.e-12))
                        break

                    if total_size > float(dataset.size) * self.block_request_max:
                        # if the total size of requested blocks is large enough, just copy the dataset
                        # covers the case where we actually have the full list of blocks (if block_request_max is less than 1)
                        item = dataset
                    else:
                        item = list(blocks)

                proto_dealer_requests.append(DealerRequest(item, group = group))

            if request.status == Request.ST_REJECTED:
                continue

            new_dealer_requests = []

            # find destinations (request.n times) for each item
            for proto_request in proto_dealer_requests:
                # try to make a dealer request for all requests, except when there is a full copy of the item

                if request.n == 0:
                    # make one copy at each site

                    for destination in sites:
                        dealer_request = DealerRequest(proto_request.item(), destination = destination)

                        if dealer_request.item_already_exists() == 2:
                            # nothing to do for this one
                            continue

                        rejection_reason = policy.check_destination(dealer_request, partition)
                        if rejection_reason is not None:
                            reject(request, 'Cannot copy %s to %s' % (dealer_request.item_name(), destination.name))
                            break
    
                        new_dealer_requests.append(dealer_request)

                    if request.status == Request.ST_REJECTED:
                        break

                else:
                    # total of n copies
                    candidate_sites = []
                    num_new = request.n

                    # bring sites where the item already exists first (may want to just "flip" the ownership)
                    sites_and_existence = []
                    for destination in sites:
                        exists = proto_request.item_already_exists(destination) # 0, 1, or 2
                        if exists != 0:
                            sites_and_existence.insert(0, (destination, exists))
                        else:
                            sites_and_existence.append((destination, exists))

                    for destination, exists in sites_and_existence:
                        if num_new == 0:
                            break

                        dealer_request = DealerRequest(proto_request.item(), destination = destination)

                        # consider copies proposed by other requests as complete
                        try:
                            proposed_blocks = blocks_to_propose[destination][dealer_request.dataset]
                        except KeyError:
                            pass
                        else:
                            if dealer_request.blocks is not None:
                                if set(dealer_request.blocks) <= proposed_blocks:
                                    num_new -= 1
                                    continue

                            else:
                                if dealer_request.dataset.blocks == proposed_blocks:
                                    num_new -= 1
                                    continue

                        # if the item already exists, it's a complete copy too
                        if exists == 2:
                            num_new -= 1
                        elif exists == 1:
                            # if the current group can be overwritten, make a request
                            # otherwise skip
                            single_owner = dealer_request.item_owned_by() # None if owned by multiple groups
                            if single_owner in overwritten_groups:
                                new_dealer_requests.append(dealer_request)
                                num_new -= 1
                        else:
                            candidate_sites.append(destination)

                    for icopy in range(num_new):
                        dealer_request = DealerRequest(proto_request.item())
                        # pick a destination randomly (weighted by available space)
                        policy.find_destination_for(dealer_request, partition, candidates = candidate_sites)
    
                        if dealer_request.destination is None:
                            # if any of the item cannot find any of the num_new destinations, reject the request
                            reject(request, 'Destination %d for %s not available' % (icopy, dealer_request.item_name()))
                            break
    
                        candidate_sites.remove(dealer_request.destination)
                        new_dealer_requests.append(dealer_request)

                # if request.n == 0, else

                if request.status == Request.ST_REJECTED:
                    break

            # for each item in request

            if request.status == Request.ST_REJECTED:
                continue

            if len(new_dealer_requests) == 0:
                # nothing to do
                request.status = Request.ST_COMPLETED
                self.request_manager.update_request(request)
                continue

            # finally add to the returned requests
            activation_list = []

            for dealer_request in new_dealer_requests:
                try:
                    site_blocks = blocks_to_propose[dealer_request.destination]
                except KeyError:
                    site_blocks = blocks_to_propose[dealer_request.destination] = {}

                if dealer_request.blocks is not None:
                    try:
                        blocks = site_blocks[dealer_request.dataset]
                    except KeyError:
                        blocks = site_blocks[dealer_request.dataset] = set()
    
                    blocks.update(dealer_request.blocks)

                    for block in dealer_request.blocks:
                        activation_list.append((block.full_name(), dealer_request.destination.name, now))

                else:
                    site_blocks[dealer_request.dataset] = set(dealer_request.dataset.blocks)

                    activation_list.append((dealer_request.item_name(), dealer_request.destination.name, now))

            # create actions and set request status to ACTIVATED
            request.activate(activation_list)
            self.request_manager.update_request(request)
            
            self.activated_requests.append(request)

        self.request_manager.unlock()

        # throw away all the DealerRequest objects we've been using and form the final proposal
        dealer_requests = []
        for site, block_list in blocks_to_propose.iteritems():
            for dataset, blocks in block_list.iteritems():
                if blocks == dataset.blocks:
                    dealer_requests.append(DealerRequest(dataset, destination = site))
                else:
                    dealer_requests.append(DealerRequest(list(blocks), destination = site))

        return dealer_requests

    def postprocess(self, cycle_number, copy_list): # override
        """
        Create active copy entries for accepted copies.
        """

        for request in self.activated_requests:
            updated = False

            for action in request.actions:
                try:
                    dataset_name, block_name = Block.from_full_name(action.item)
                except ObjectError:
                    dataset_name = action.item
                    block_name = None
                    
                for replica in copy_list:
                    if replica.site.name != action.site:
                        continue

                    if replica.growing:
                        # full dataset copy - dataset and block requests are both queued
                        if dataset_name == replica.dataset.name:
                            action.status = RequestAction.ST_QUEUED

                    else:
                        # match block-by-block
                        if block_name is None:
                            # dataset request
                            continue

                        for block_replica in replica.block_replicas:
                            if block_name == block_replica.block.real_name():
                                action.status = RequestAction.ST_QUEUED
                                break

                    if action.status == RequestAction.ST_QUEUED:
                        updated = True
                        # action got queued - no need to check other replicas
                        break

            if updated:
                self.request_manager.update_request(request)
