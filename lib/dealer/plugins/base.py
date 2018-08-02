from dynamo.dataformat import Dataset, Block, OperationalError

class BaseHandler(object):
    def __init__(self, name):
        self.name = name
        self.required_attrs = []
        self._read_only = False

    def set_read_only(self, value = True):
        self._read_only = value

    def get_requests(self, inventory, policy):
        """
        Return a prioritized list of objects requesting transfer of.
        @param inventory  DynamoInventory object.
        @param policy     DealerPolicy object

        @return List of DealerRequests.
        """

        return []

    def postprocess(self, cycle_number, copy_list):
        """
        Do post-request processing.
        @param cycle_number   Dealer cycle number
        @param copy_list      List of DatasetReplicas
        """
        pass


class DealerRequest(object):
    __slots__ = ['dataset', 'block', 'blocks', 'destination', 'group']

    def __init__(self, item, destination = None, group = None):
        """
        @param item         Dataset, Block, or a list of Blocks.
        @param destination  A site object
        @param group        A group object
        """

        if type(item) is Dataset:
            self.dataset = item
            self.block = None
            self.blocks = None

        elif type(item) is Block:
            self.dataset = item.dataset
            self.block = item
            self.blocks = None

        elif type(item) is list:
            if len(item) == 0:
                raise OperationalError('DealerRequest instantiated with an empty list')

            self.dataset = item[0].dataset
            for block in item[1:]:
                if block.dataset is not self.dataset:
                    raise OperationalError('DealerRequest instantiated with a list of blocks from mixed datasets')

            self.blocks = item
            self.block = None

        else:
            raise OperationalError('Invalid object passed to DealerRequest')

        self.destination = destination
        self.group = group

    def item(self):
        if self.block is not None:
            return self.block
        elif self.blocks is not None:
            return self.blocks
        else:
            return self.dataset

    def item_name(self):
        if self.block is not None:
            return self.block.full_name()
        else:
            return self.dataset.name

    def item_size(self):
        if self.block is not None:
            return self.block.size
        elif self.blocks is not None:
            return sum(block.size for block in self.blocks)
        else:
            return self.dataset.size

    def item_already_exists(self, site = None):
        """
        Check item existence at the site. Group is set when this function is called.
        Return values are
        0 -> item does not exist at the site
        1 -> item exists but is owned by a different group
        2 -> item exists and is owned by the group
        @param site   Site to be checked.

        @return Return code as defined above.
        """

        if site is None:
            site = self.destination # must not be None!

        level = 0

        if self.block is not None:
            replica = site.find_block_replica(self.block)
            if replica is not None and replica.is_complete():
                level = 1
                if replica.group == self.group:
                    level = 2
        
        elif self.blocks is not None:
            complete_at_site = True
            owned_at_site = True
        
            for block in self.blocks:
                replica = site.find_block_replica(block)
                if replica is None or not replica.is_complete():
                    complete_at_site = False
                elif replica.group != self.group:
                    owned_at_site = False
        
            if complete_at_site:
                if owned_at_site:
                    level = 2
                else:
                    level = 1
            else:
                level = 0

        else:
            replica = site.find_dataset_replica(self.dataset)
            if replica is not None and replica.is_full():
                level = 1
        
                owners = set(brep.group for brep in replica.block_replicas)
                if len(owners) == 1 and list(owners)[0] == self.group:
                    level = 2

        return level

    def item_owned_by(self):
        """
        "group finding" functions
        If the item is owned by a single group at the site, return the group object
        Otherwise return None

        @return Owning group or None
        """

        group = None

        if self.block is not None:
            replica = self.destination.find_block_replica(self.block)
            if replica is not None:
                group = replica.group

        elif self.blocks is not None:
            for block in blocks:
                replica = self.destination.find_block_replica(block)
                if replica is None:
                    if group is None:
                        group = replica.group
                    else:
                        return None

        else:
            replica = self.destination.find_dataset_replica(self.dataset)
            if replica is not None:
                owners = set(brep.group for brep in replica.block_replicas)
                if len(owners) == 1:
                    group = list(owners)[0]

        return group


