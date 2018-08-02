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

    @property
    def num_files(self):
        if self.file_ids is None:
            return self.block.num_files
        else:
            return len(self.file_ids)

    def __init__(self, block, site, group, is_custodial = False, size = -1, last_update = 0, file_ids = None):
        # User of the object is responsible for making sure size and file_ids are consistent
        # if _use_file_ids is True, file_ids should be a tuple of (long) integers or LFN strings,
        #   latter in case where the file is not yet registered with the inventory
        # if _use_file_ids is False, file_ids is the number of files this replica has.

        self._block = block
        self._site = site
        self.group = group
        self.is_custodial = is_custodial
        self.last_update = last_update

        # Override file_ids depending on the given size:
        # If size < 0, this replica is considered full. If type(block) is Block, set the size and file_ids
        #  from the block. If not, this is a transient object - just set the size to -1.
        # If size == 0 and file_ids is None, self.file_ids becomes an empty tuple.
        #  size == 0 and file_ids = finite tuple is allowed. It is the object creator's responsibility to
        #  ensure that the files in the provided list are all 0-size.

        if size < 0:
            if type(block) is Block:
                self.size = block.size
                if BlockReplica._use_file_ids:
                    self.file_ids = None
                else:
                    self.file_ids = block.num_files
            else:
                self.size = -1
                self.file_ids = None

        elif size == 0 and file_ids is None:
            self.size = 0
            if BlockReplica._use_file_ids:
                self.file_ids = tuple()
            else:
                self.file_ids = 0

        elif file_ids is None:
            if type(block) is not Block:
                raise ObjectError('Cannot initialize a BlockReplica with finite size and file_ids = None without a valid block')

            if size == block.size:
                self.size = size
                if BlockReplica._use_file_ids:
                    self.file_ids = None
                else:
                    self.file_ids = block.num_files
            else:
                raise ObjectError('BlockReplica file_ids cannot be None when size is finite and not the full block size')

        else:
            self.size = size

            if BlockReplica._use_file_ids:
                # some iterable
                tmplist = []
                for fid in file_ids:
                    if type(self._block) is not str and type(fid) is str:
                        tmplist.append(self._block.find_file(fid, must_find = True).id)
                    else:
                        tmplist.append(fid)
    
                self.file_ids = tuple(tmplist)
            else:
                # must be an integer
                self.file_ids = file_ids

    def __str__(self):
        return 'BlockReplica %s:%s (group=%s, size=%d, last_update=%s)' % \
            (self._site_name(), self._block_full_name(),
                self._group_name(), self.size,
                time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(self.last_update)))

    def __repr__(self):
        # repr is mostly used for application - server communication. Set size to -1 if the replica is complete
        if self.is_complete():
            size = -1
            file_ids = None
        else:
            size = self.size
            file_ids = self.file_ids

        return 'BlockReplica(%s,%s,%s,%s,%d,%d,%s)' % \
            (repr(self._block_full_name()), repr(self._site_name()), repr(self._group_name()), \
            self.is_custodial, size, self.last_update, repr(file_ids))

    def __eq__(self, other):
        if BlockReplica._use_file_ids:
            # check len() first to avoid having to create sets for no good reason
            if (self.file_ids is None and other.file_ids is not None) or \
               (self.file_ids is not None and other.file_ids is None):
                return False

            file_ids_match = (self.file_ids == other.file_ids) or ((len(self.file_ids) == len(other.file_ids)) and (set(self.file_ids) == set(other.file_ids)))
        else:
            file_ids_match = self.file_ids == other.file_ids

        return self is other or \
            (self._block_full_name() == other._block_full_name() and self._site_name() == other._site_name() and \
             self._group_name() == other._group_name() and \
             self.is_custodial == other.is_custodial and self.size == other.size and \
             self.last_update == other.last_update and file_ids_match)

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
            if self.size < 0:
                # self represents a full block replica without the knowledge of the actual size (again an unlinked clone)
                replica.size = block.size

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
        if BlockReplica._use_file_ids and self.file_ids is not None:
            for fid in self.file_ids:
                try:
                    fid += 0
                except TypeError:
                    # was some string
                    raise ObjectError('Cannot write %s into store because one of the files %s %s is not known yet' % (str(self), fid, type(fid).__name__))

        store.save_blockreplica(self)

    def delete_from(self, store):
        store.delete_blockreplica(self)

    def is_complete(self):
        size_match = (self.size == self._block.size)
        if BlockReplica._use_file_ids:
            if self.file_ids is None:
                return True
            else:
                # considering the case where we are missing zero-size files
                return size_match and (len(self.file_ids) == self._block.num_files)
        else:
            return size_match and (self.file_ids == self._block.num_files)

    def files(self):
        if not BlockReplica._use_file_ids:
            raise NotImplementedError('BlockReplica.files')

        block_files = self.block.files
        if self.file_ids is None:
            return set(block_files)
        else:
            by_id = dict((f.id, f) for f in block_files if f.id != 0)
            result = set()
            for fid in self.file_ids:
                try:
                    fid += 0
                except TypeError:
                    # fid is lfn
                    result.add(self._block.find_file(fid))
                else:
                    result.add(by_id[fid])

            return result

    def has_file(self, lfile):
        if lfile.block is not self.block:
            return False

        if self.file_ids is None:
            return True

        if lfile.id == 0:
            for f in self.files():
                if f.lfn == lfile.lfn:
                    return True

            return False

        else:
            return lfile.id in self.file_ids

    def add_file(self, lfile):
        if lfile.block != self.block:
            raise ObjectError('Cannot add file %s (block %s) to %s', lfile.lfn, lfile.block.full_name(), str(self))

        if BlockReplica._use_file_ids:
            if self.file_ids is None:
                # This was a full replica. A new file was added to the block. The replica remains full.
                return
            else:
                file_ids = set(self.file_ids)

            if lfile.id == 0:
                if lfile.lfn in file_ids:
                    return
                if lfile.lfn in set(f.lfn for f in self.files()):
                    return
                file_ids.add(lfile.lfn)
            else:
                if lfile.id in file_ids:
                    return
                file_ids.add(lfile.id)

            self.size += lfile.size
    
            if self.size == self.block.size and len(file_ids) == self.block.num_files:
                self.file_ids = None
            else:
                self.file_ids = tuple(file_ids)

        else:
            self.file_ids += 1

    def delete_file(self, lfile, full_deletion = False):
        """
        Delete a file from the replica.
        @param  lfile          A File object
        @param  full_deletion  Set to True if the file is being deleted from the block as well.

        @return  True if the file is in the replica.
        """

        if lfile.block != self.block:
            raise ObjectError('Cannot delete file %s (block %s) from %s', lfile.lfn, lfile.block.full_name(), str(self))

        if BlockReplica._use_file_ids:
            if lfile.id == 0:
                identifier = lfile.lfn
            else:
                identifier = lfile.id

            if self.file_ids is None:
                if full_deletion:
                    # file is being deleted from the block as well. Full replica remains full.
                    self.size -= lfile.size
                    return True
                else:                    
                    file_ids = [(f.id if f.id != 0 else f.lfn) for f in self.block.files]

            elif identifier not in self.file_ids:
                return False

            else:
                file_ids = list(self.file_ids)

            file_ids.remove(identifier)
            self.file_ids = tuple(file_ids)

        else:
            self.file_ids -= 1

        self.size -= lfile.size

        return True

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

    def _copy_no_check(self, other):
        self.group = other.group
        self.is_custodial = other.is_custodial
        self.size = other.size
        self.last_update = other.last_update

        if BlockReplica._use_file_ids:
            if other.file_ids is None:
                self.file_ids = None
            else:
                tmplist = []
                for fid in other.file_ids:
                    try:
                        fid += 0
                    except TypeError:
                        lfn = fid
                        fid = Block.inventory_store.get_file_id(lfn)
                        if fid is None:
                            # file not in store yet
                            tmplist.append(lfn)
                        else:
                            tmplist.append(fid)
                    else:
                        tmplist.append(fid)
    
                self.file_ids = tuple(tmplist)

        else:
            self.file_ids = other.file_ids
