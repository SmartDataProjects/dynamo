import time

from exceptions import ObjectError
from block import Block
from _namespace import customize_blockreplica

class BlockReplica(object):
    """Block placement at a site. Holds an attribute 'group' which can be None.
    BlockReplica size can be different from that of the Block."""

    __slots__ = ['_block', '_site', 'group', 'is_custodial', 'size', 'last_update', 'file_ids']

    _use_file_ids = True

    @property
    def block(self):
        return self._block

    @property
    def site(self):
        return self._site

    def __init__(self, block, site, group, is_custodial = False, size = -1, last_update = 0, file_ids = None):
        # Creater of the object is responsible for making sure size and file_ids are consistent
        # file_ids should be a tuple of (long) integers or LFN strings, latter in case where the file is not yet registered with the inventory

        self._block = block
        self._site = site
        self.group = group
        self.is_custodial = is_custodial
        if size < 0 and type(block) is not str:
            self.size = block.size
        else:
            self.size = size
        self.last_update = last_update

        # tuple of file ids for incomplete replicas
        if file_ids is None:
            self.file_ids = None
        else:
            # some iterable
            tmplist = []
            for fid in file_ids:
                if type(fid) is str:
                    tmplist.append(self._block.find_file(fid, must_find = True).id)
                else:
                    tmplist.append(fid)

            self.file_ids = tuple(tmplist)

    def __str__(self):
        return 'BlockReplica %s:%s (group=%s, size=%d, last_update=%s)' % \
            (self._site_name(), self._block_full_name(),
                self._group_name(), self.size,
                time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(self.last_update)))

    def __repr__(self):
        return 'BlockReplica(%s,%s,%s,%s,%d,%d,%s)' % \
            (repr(self._block_full_name()), repr(self._site_name()), repr(self._group_name()), \
            self.is_custodial, self.size, self.last_update, repr(self.file_ids))

    def __eq__(self, other):
        return self is other or \
            (self._block_full_name() == other._block_full_name() and self._site_name() == other._site_name() and \
            self._group_name() == other._group_name() and \
            self.is_custodial == other.is_custodial and self.size == other.size and \
             self.last_update == other.last_update and self.file_ids == other.file_ids)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        if self._block_full_name() != other._block_full_name():
            raise ObjectError('Cannot copy a replica of %s into a replica of %s' % (other._block_full_name(), self._block_full_name()))
        if self._site_name() != other._site_name():
            raise ObjectError('Cannot copy a replica at %s into a replica at %s' % (other._site.name, self._site_name()))

        self._copy_no_check(other)

    def embed_into(self, inventory, check = False):
        try:
            dataset = inventory.datasets[self._dataset_name()]
        except KeyError:
            raise ObjectError('Unknown dataset %s' % (self._dataset_name()))

        block = dataset.find_block(self._block_name(), must_find = True)

        try:
            site = inventory.sites[self._site_name()]
        except KeyError:
            raise ObjectError('Unknown site %s' % (self._site_name()))

        try:
            group = inventory.groups[self._group_name()]
        except KeyError:
            raise ObjectError('Unknown group %s' % (self._group_name()))

        replica = block.find_replica(site)
        updated = False
        if replica is None:
            replica = BlockReplica(block, site, group, self.is_custodial, self.size, self.last_update, self.file_ids)
    
            dataset_replica = site.find_dataset_replica(dataset, must_find = True)
            dataset_replica.block_replicas.add(replica)
            block.replicas.add(replica)
            site.add_block_replica(replica)

            updated = True
        elif check and (replica is self or replica == self):
            # identical object -> return False if check is requested
            pass
        else:
            replica.copy(self)
            if type(self.group) is str or self.group is None:
                # can happen if self is an unlinked clone
                replica.group = group

            site.update_partitioning(replica)
            updated = True

        if check:
            return replica, updated
        else:
            return replica

    def unlink_from(self, inventory):
        try:
            dataset = inventory.datasets[self._dataset_name()]
            block = dataset.find_block(self._block_name(), must_find = True)
            site = inventory.sites[self._site_name()]
            replica = block.find_replica(site, must_find = True)
        except (KeyError, ObjectError):
            return None

        replica.unlink()
        return replica

    def unlink(self, dataset_replica = None, unlink_dataset_replica = True):
        if dataset_replica is None:
            dataset_replica = self._site.find_dataset_replica(self._block._dataset, must_find = True)

        for site_partition in self._site.partitions.itervalues():
            try:
                block_replicas = site_partition.replicas[dataset_replica]
            except KeyError:
                continue

            if block_replicas is None:
                # site_partition contained all block replicas. It will contain all after a deletion.
                continue

            try:
                block_replicas.remove(self)
            except KeyError:
                # this replica was not part of the partition
                continue

            if len(block_replicas) == 0:
                site_partition.replicas.pop(dataset_replica)

        dataset_replica.block_replicas.remove(self)
        if unlink_dataset_replica and not dataset_replica.growing and len(dataset_replica.block_replicas) == 0:
            dataset_replica.unlink()

        self._block.replicas.remove(self)

    def write_into(self, store):
        store.save_blockreplica(self)

    def delete_from(self, store):
        store.delete_blockreplica(self)

    def is_complete(self):
        return self.size == self.block.size

    def files(self):
        block_files = self.block.files
        if self.file_ids is None:
            return set(block_files)
        else:
            return set(f for f in block_files if f.id in self.file_ids)

    def _block_full_name(self):
        if type(self._block) is str:
            return self._block
        else:
            return self._block.full_name()

    def _block_real_name(self):
        if type(self._block) is str:
            return Block.to_real_name(Block.from_full_name(self._block)[1])
        else:
            return self._block.real_name()

    def _block_name(self):
        if type(self._block) is str:
            return Block.from_full_name(self._block)[1]
        else:
            return self._block.name

    def _dataset_name(self):
        if type(self._block) is str:
            return Block.from_full_name(self._block)[0]
        else:
            return self._block.dataset.name

    def _site_name(self):
        if type(self._site) is str:
            return self._site
        else:
            return self._site.name

    def _group_name(self):
        if type(self.group) is str or self.group is None:
            return self.group
        else:
            return self.group.name

    def _copy_no_check(self):
        self.group = other.group
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update
        if other.file_ids is None:
            self.file_ids = None
        else:
            self.file_ids = tuple(other.file_ids)
