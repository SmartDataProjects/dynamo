import sys

from exceptions import IntegrityError

class SitePartition(object):
    """State of a partition at a site."""

    __slots__ = ['_site', '_partition', '_quota', 'replicas']

    @property
    def site(self):
        return self._site

    @property
    def partition(self):
        return self._partition

    @property
    def quota(self):
        if self._partition.subpartitions is not None:
            q = 0
            for p in self._partition.subpartitions:
                try:
                    q += self._site.partitions[p].quota
                except KeyError:
                    # can happen in certain fringe cases
                    pass

            return q

        else:
            return self._quota

    def __init__(self, site, partition, quota = 0.):
        self._site = site
        self._partition = partition
        # partition quota in bytes
        self._quota = quota
        # {dataset_replica: set(block_replicas) or None (if all blocks are in)}
        self.replicas = {}

    def __str__(self):
        return 'SitePartition %s/%s (quota=%f TB, occupancy %s)' % (self._site.name, self._partition.name, \
            self.quota * 1.e-12, ('%.2f' % self.occupancy_fraction()) if self.quota != 0. else 'inf')

    def __repr__(self):
        return 'SitePartition(%s, %s)' % (repr(self._site), repr(self._partition))

    def __eq__(self, other):
        return self._site.name == other._site.name and self._partition.name == other._partition.name and self._quota == other._quota

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._site.name != other._site.name:
            raise ObjectError('Cannot copy a partition at %s into a partition at %s', other._site.name, self._site.name)
        if self._partition.name != other._partition.name:
            raise ObjectError('Cannot copy a site partition of %s into a site partition of %s', other._partition.name, self._partition.name)

        self._quota = other._quota

    def unlinked_clone(self):
        site = self._site.unlinked_clone()
        partition = self._partition.unlinked_clone()
        return SitePartition(site, partition, self._quota)

    def embed_into(self, inventory, check = False):
        try:
            site = inventory.sites[self._site.name]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site.name)

        try:
            partition = inventory.partitions[self._partition.name]
        except KeyError:
            raise ObjectError('Unknown partition %s', self._partition.name)

        updated = False

        try:
            site_partition = site.partitions[partition]
        except KeyError:
            site_partition = SitePartition(site, partition)
            site_partition.copy(self)
            site.partitions[partition] = site_partition
            updated = True

        else:            
            if check and (site_partition is self or site_partition == self):
                # identical object -> return False if check is requested
                pass
            else:
                site_partition.copy(self)
                updated = True

        if check:
            return site_partition, updated
        else:
            return site_partition

    def delete_from(self, inventory):
        raise ObjectError('Deleting a single SitePartition is not allowed.')

    def write_into(self, store, delete = False):
        if delete:
            store.delete_sitepartition(self)
        else:
            store.save_sitepartition(self)

    def set_quota(self, quota):
        if self._partition.subpartitions is not None:
            # this is a superpartition
            raise IntegrityError('Cannot set quota on a superpartition.')

        if self._partition.parent is not None and quota < 0:
            # quota < 0 -> infinite. This partition cannot be a subpartition
            raise IntegrityError('Infinite quota set for a subpartition')

        self._quota = quota

    def occupancy_fraction(self, physical = True):
        quota = self.quota

        if quota == 0.:
            return sys.float_info.max
        elif quota < 0.:
            return 0.
        else:
            total_size = 0.
            for replica, block_replicas in self.replicas.iteritems():
                if block_replicas is None:
                    total_size += replica.size(physical = physical)
                elif physical:
                    total_size += sum(br.size for br in block_replicas)
                else:
                    total_size += sum(br.block.size for br in block_replicas)

            return total_size / quota
