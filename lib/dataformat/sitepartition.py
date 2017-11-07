import sys

from dataformat.exceptions import IntegrityError

class SitePartition(object):
    """State of a partition at a site."""

    __slots__ = ['site', 'partition', 'quota', 'replicas']

    def __init__(self, site, partition, quota = 0.):
        self.site = site
        self.partition = partition
        self.quota = quota
        # {dataset_replica: set(block_replicas) or None (if all blocks are in)}
        self.replicas = {}

    def __str__(self):
        return 'SitePartition %s/%s (quota %f, occupancy %f)' % (self.site.name, self.partition.name, self.quota, self.occupancy_fraction())

    def __repr__(self):
        return 'SitePartition(%s, %s)' % (repr(self.site), repr(self.partition))

    def copy(self, other):
        self.quota = other.quota

    def unlinked_clone(self):
        site = self.site.unlinked_clone()
        partition = self.partition.unlinked_clone()
        return SitePartition(site, partition, self.quota)

    def linked_clone(self, inventory):
        """Does not clone replicas."""

        site = inventory.sites[self.site.name]
        partition = inventory.partitions[self.partition.name]

        site_partition = SitePartition(site, partition, self.quota)

        site.partitions[partition] = site_partition

        return site_partition

    def set_quota(self, quota):
        if self.partition.parent is not None:
            # this is a subpartition. Update the parent partition quota
            if quota < 0:
                # quota < 0 -> infinite. This partition cannot be a subpartition
                raise IntegrityError('Infinite quota set for a subpartition')

            self.site.partitions[self.partition.parent].quota += quota - self.quota

        self.quota = quota

    def occupancy_fraction(self, physical = True):
        if self.quota == 0.:
            return sys.float_info.max
        elif self.quota < 0.:
            return 0.
        else:
            total_size = 0.
            for replica, block_replicas in self.replicas.iteritems():
                if block_replicas is None:
                    total_size += replica.size(physica = physical)
                elif physical:
                    total_size += sum(br.size for br in block_replicas)
                else:
                    total_size += sum(br.block.size for br in block_replicas)

            return total_size / self.quota
