class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    __slots__ = ['dataset', 'site', 'is_complete', 'is_custodial', 'last_block_created', 'block_replicas']

    def __init__(self, dataset, site, is_complete = False, is_custodial = False, last_block_created = 0):
        self.dataset = dataset
        self.site = site
        self.is_complete = is_complete # = complete subscription. Can still be partial
        self.is_custodial = is_custodial
        self.last_block_created = last_block_created
        self.block_replicas = set()

    def __str__(self):
        return 'DatasetReplica {site}:{dataset} (is_complete={is_complete}, is_custodial={is_custodial},' \
            ' {block_replicas_size} block_replicas)'.format(
                site = self.site.name, dataset = self.dataset.name, is_complete = self.is_complete,
                is_custodial = self.is_custodial,
                block_replicas_size = len(self.block_replicas))

    def __repr__(self):
        return 'DatasetReplica(%s, %s)' % (repr(self.dataset), repr(self.site))

    def is_last_copy(self):
        return len(self.dataset.replicas) == 1 and self.dataset.replicas[0] == self

    def is_partial(self):
        # dataset.blocks must be loaded if a replica is created for the dataset
        return self.is_complete and len(self.block_replicas) != len(self.dataset.blocks)

    def is_full(self):
        # dataset.blocks must be loaded if a replica is created for the dataset
        return self.is_complete and len(self.block_replicas) == len(self.dataset.blocks)

    def size(self, groups = [], physical = True):
        if type(groups) is not list:
            # single group given
            if physical:
                return sum([r.size for r in self.block_replicas if r.group == groups])
            else:
                return sum([r.block.size for r in self.block_replicas if r.group == groups])

        else: # expect a list
            if len(groups) == 0:
                # no group spec
                if self.is_full():
                    return self.dataset.size
                else:
                    if physical:
                        return sum([r.size for r in self.block_replicas])
                    else:
                        return sum([r.block.size for r in self.block_replicas])

            else:
                if physical:
                    return sum([r.size for r in self.block_replicas if r.group in groups])
                else:
                    return sum([r.block.size for r in self.block_replicas if r.group in groups])

    def find_block_replica(self, block):
        try:
            if type(block).__name__ == 'Block':
                return next(b for b in self.block_replicas if b.block == block)
            else:
                return next(b for b in self.block_replicas if b.block.name == block)

        except StopIteration:
            return None

    def remove_block_replica(self, block_replica):
        self.block_replicas.remove(block_replica)
        self.site.update_partitioning(self)
