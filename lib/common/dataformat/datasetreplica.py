from blockreplica import BlockReplica

class DatasetReplica(object):
    """Represents a dataset replica. Combines dataset and site information."""

    __slots__ = ['dataset', 'site', 'is_complete', 'is_custodial', 'last_block_created', 'block_replicas']

    def __init__(self, dataset, site, is_complete = False, is_custodial = False, last_block_created = 0):
        self.dataset = dataset
        self.site = site
        self.is_complete = is_complete # = complete subscription. Can still be partial
        self.is_custodial = is_custodial
        self.last_block_created = last_block_created
        self.block_replicas = []

    def __str__(self):
        return 'DatasetReplica {site}:{dataset} (is_complete={is_complete}, is_custodial={is_custodial},' \
            ' {block_replicas_size} block_replicas)'.format(
                site = self.site.name, dataset = self.dataset.name, is_complete = self.is_complete,
                is_custodial = self.is_custodial,
                block_replicas_size = len(self.block_replicas))

    def __repr__(self):
        rep = 'DatasetReplica(%s,\n' % repr(self.dataset)
        rep += '    %s,\n' % repr(self.site)
        rep += '    is_complete=%s,\n' % str(self.is_complete)
        rep += '    is_custodial=%s,\n' % str(self.is_custodial)
        rep += '    last_block_created=%d)' % self.last_block_created

        return rep

    def unlink(self):
        # Detach this replica from owning containers but retain references from this replica

        self.dataset.replicas.remove(self)

        self.site.dataset_replicas.remove(self)

        for block_replica in self.block_replicas:
            self.site.remove_block_replica(block_replica)

    def link(self):
        # Reverse operation of unlink

        self.dataset.replicas.append(self)

        self.site.dataset_replicas.add(self)

        for block_replica in self.block_replicas:
            self.site.add_block_replica(block_replica)

    def clone(self, block_replicas = True):
        # Create a detached clone. Detached in the sense that it is not linked from dataset or site.
        replica = DatasetReplica(dataset = self.dataset, site = self.site, is_complete = self.is_complete, is_custodial = self.is_custodial, last_block_created = self.last_block_created)

        if block_replicas:
            for brep in self.block_replicas:
                replica.block_replicas.append(brep.clone())

        return replica

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

    def update_block_replica(self, block, group, is_complete, is_custodial, size, last_update):
        old_replica = next(b for b in self.block_replicas if b.block == block)
        self.block_replicas.remove(old_replica)

        new_replica = BlockReplica(
            block,
            self.site,
            group,
            is_complete, 
            is_custodial,
            size = size,
            last_update = last_update
        )
        self.block_replicas.append(new_replica)

        self.site.remove_block_replica(old_replica)
        self.site.add_block_replica(new_replica)
