import sys

from dataformat.exceptions import IntegrityError

class SitePartition(object):
    """State of a partition at a site."""

    __slots__ = ['_site', '_partition', 'quota', 'replicas']

    @property
    def site(self):
        return self._site

    @property
    def partition(self):
        return self._partition

    def __init__(self, site, partition, quota = 0.):
        self._site = site
        self._partition = partition
        self.quota = quota
        # {dataset_replica: set(block_replicas) or None (if all blocks are in)}
        self.replicas = {}

    def __str__(self):
        return 'SitePartition %s/%s (quota %f, occupancy %f)' % (self._site.name, self._partition.name, self.quota, self.occupancy_fraction())

    def __repr__(self):
        return 'SitePartition(%s, %s)' % (repr(self._site), repr(self._partition))

    def __eq__(self, other):
        return self._site is other._site and self._partition is other._partition and self.quota == other.quota

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        # does not copy replicas
        self.quota = other.quota

    def unlinked_clone(self):
        site = self._site.unlinked_clone()
        partition = self._partition.unlinked_clone()
        return SitePartition(site, partition, self.quota)

    def embed_into(self, inventory, check = False):
        try:
            site = inventory.sites[self._site.name]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site.name)

        try:
            partition = inventory.partitions[self._partition.name]
        except KeyError:
            raise ObjectError('Unknown partition %s', self._partition.name)

        site_partition = site.partitions[partition]

        if check and site_partition == self:
            return False
        else:
            site_partition.copy(self)
            return True

    def delete_from(self, inventory):
        raise ObjectError('Deleting a single SitePartition is not allowed.')

    def set_quota(self, quota):
        if self._partition.parent is not None:
            # this is a subpartition. Update the parent partition quota
            if quota < 0:
                # quota < 0 -> infinite. This partition cannot be a subpartition
                raise IntegrityError('Infinite quota set for a subpartition')

            self._site.partitions[self._partition.parent].quota += quota - self.quota

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
