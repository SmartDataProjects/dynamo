import sys

from exceptions import ObjectError, IntegrityError

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
        return 'SitePartition %s/%s (quota=%f TB, occupancy %s)' % (self._site_name(), self._partition_name(), \
            self.quota * 1.e-12, ('%.2f' % self.occupancy_fraction()) if self.quota != 0. else 'inf')

    def __repr__(self):
        return 'SitePartition(%s, %s)' % (repr(self._site), repr(self._partition))

    def __eq__(self, other):
        return self is other or \
            (self._site_name() == other._site_name() and self._partition_name() == other._partition_name() and self._quota == other._quota)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._site_name() != other._site_name():
            raise ObjectError('Cannot copy a partition at %s into a partition at %s', other._site_name(), self._site_name())
        if self._partition_name() != other._partition_name():
            raise ObjectError('Cannot copy a site partition of %s into a site partition of %s', other._partition_name(), self._partition_name())

        self._quota = other._quota

    def unlinked_clone(self, attrs = True):
        if attrs:
            return SitePartition(self._site_name(), self._partition_name(), self._quota)
        else:
            return SitePartition(self._site_name(), self._partition_name())

    def embed_into(self, inventory, check = False):
        try:
            site = inventory.sites[self._site_name()]
        except KeyError:
            raise ObjectError('Unknown site %s', self._site_name())

        try:
            partition = inventory.partitions[self._partition_name()]
        except KeyError:
            raise ObjectError('Unknown partition %s', self._partition_name())

        updated = False

        try:
            site_partition = site.partitions[partition]
        except KeyError:
            IntegrityError('SitePartition %s/%s must exist but does not.', site.name, partition.name)
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

    def unlink_from(self, inventory):
        raise ObjectError('Deletion of a single SitePartition is not allowed.')

    def write_into(self, store):
        store.save_sitepartition(self)

    def delete_from(self, store):
        raise ObjectError('Deletion of a single SitePartition is not allowed.')

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

    def embed_tree(self, inventory):
        if self._partition._subpartitions is not None:
            for subp in self._partition._subpartitions:
                self._site.partitions[subp].embed_tree(inventory)

        return self.embed_into(inventory)

    def _site_name(self):
        if type(self._site) is str:
            return self._site
        else:
            return self._site.name

    def _partition_name(self):
        if type(self._partition) is str:
            return self._partition
        else:
            return self._partition.name
