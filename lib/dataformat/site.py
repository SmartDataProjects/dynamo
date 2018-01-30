from exceptions import ObjectError, IntegrityError
from sitepartition import SitePartition

class Site(object):
    """Represents a site. Owns lists of dataset and block replicas, which are organized into partitions."""

    __slots__ = ['_name', 'host', 'storage_type', 'backend',
        'storage', 'cpu', 'status',
        '_dataset_replicas', 'partitions']

    _storage_types = ['disk', 'mss', 'buffer', 'unknown']
    TYPE_DISK, TYPE_MSS, TYPE_BUFFER, TYPE_UNKNOWN = range(1, len(_storage_types) + 1)
    _statuses = ['ready', 'waitroom', 'morgue', 'unknown']
    STAT_READY, STAT_WAITROOM, STAT_MORGUE, STAT_UNKNOWN = range(1, len(_statuses) + 1)

    @property
    def name(self):
        return self._name

    @staticmethod
    def storage_type_val(arg):
        if type(arg) is str:
            return eval('Site.TYPE_' + arg.upper())
        else:
            return arg

    @staticmethod
    def storage_type_name(arg):
        if type(arg) is int:
            return Site._storage_types[arg - 1]
        else:
            return arg

    @staticmethod
    def status_val(arg):
        if type(arg) is str:
            return eval('Site.STAT_' + arg.upper())
        else:
            return arg

    @staticmethod
    def status_name(arg):
        if type(arg) is int:
            return Site._statuses[arg - 1]
        else:
            return arg

    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', storage = 0., cpu = 0., status = STAT_UNKNOWN):
        self._name = name
        self.host = host
        if type(storage_type) is str:
            storage_type = Site.storage_type_val(storage_type)
        self.storage_type = storage_type
        self.backend = backend
        self.storage = storage # in TB
        self.cpu = cpu # in kHS06
        self.status = status

        self._dataset_replicas = {} # {Dataset: DatasetReplica}

        self.partitions = {} # {Partition: SitePartition}

    def __str__(self):
        return 'Site %s (host=%s, storage_type=%s, backend=%s, storage=%d, cpu=%f, status=%s)' % \
            (self._name, self.host, Site.storage_type_name(self.storage_type), self.backend, self.storage, self.cpu, Site.status_name(self.status))

    def __repr__(self):
        return 'Site(\'%s\')' % self._name

    def __eq__(self, other):
        return self is other or \
            (self._name == other._name and self.host == other.host and self.storage_type == other.storage_type and \
            self.backend == other.backend and self.storage == other.storage and self.cpu == other.cpu and \
            self.status == other.status)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        """Only copy simple member variables."""

        self.host = other.host
        self.storage_type = other.storage_type
        # Temporarily commenting out to not overwrite what Max collected
        # self.backend = other.backend
        self.storage = other.storage
        self.cpu = other.cpu
        self.status = other.status

    def unlinked_clone(self, attrs = True):
        if attrs:
            return Site(self._name, self.host, self.storage_type, self.backend, self.storage, self.cpu, self.status)
        else:
            return Site(self._name)

    def embed_into(self, inventory, check = False):
        updated = False

        try:
            site = inventory.sites[self._name]
        except KeyError:
            site = self.unlinked_clone()
            inventory.sites.add(site)

            # Special case: automatically createing new site partitions.
            # In write-enabled applications, inventory will add the newly created
            # site clone into _updated_objects after this function returns.
            # To have site partitions also added to _updated_objects *after* the
            # site is added to the list, we need to call the update() back from within.

            # Just set some value off so updated is triggered
            site.status = self.status + 1
            inventory.update(self)

            # Now site is saved in inventory._updated_objects

            for partition in inventory.partitions.itervalues():
                inventory.update(SitePartition(site, partition))

            # Reporting updated = True causes the site to be added to _updated_objects twice,
            # but this is the only way to trigger writing update by the server
            updated = True
            # site_partitions will always be written in Site.write_into.

        else:
            if check and (site is self or site == self):
                # identical object -> return False if check is requested
                pass
            else:
                site.copy(self)
                updated = True

        if check:
            return site, updated
        else:
            return site

    def delete_from(self, inventory):
        # Pop the site from the main list, and remove all replicas on the site.
        site = inventory.sites.pop(self._name)

        deleted = []

        for replica in site._dataset_replicas.values():
            deleted.extend(replica._unlink())

        deleted.append(site)

        return deleted

    def write_into(self, store, delete = False):
        if delete:
            store.delete_site(self)
        else:
            store.save_site(self)

        for site_partition in self.partitions.itervalues():
            site_partition.write_into(store, delete = delete)

    def find_dataset_replica(self, dataset, must_find = False):
        try:
            return self._dataset_replicas[dataset]
        except KeyError:
            if must_find:
                raise ObjectError('Could not find replica of %s in %s', dataset.name, self._name)
            else:
                return None

    def find_block_replica(self, block, must_find = False):
        if type(block).__name__ == 'Block':
            try:
                dataset_replica = self._dataset_replicas[block.dataset]
            except KeyError:
                if must_find:
                    raise ObjectError('Could not find replica of %s in %s', block.dataset.name, self._name)
                else:
                    return None
            else:
                return dataset_replica.find_block_replica(block, must_find = must_find)
        else:
            # lookup by block name - very inefficient operation
            for dataset_replica in self._dataset_replicas.itervalues():
                for block_replica in dataset_replica.block_replicas:
                    if block_replica.block.name == block:
                        return block_replica

            if must_find:
                raise ObjectError('Could not find replica of %s in %s', block.full_name(), self._name)
            else:
                return None

    def dataset_replicas(self):
        return self._dataset_replicas.itervalues()

    def add_dataset_replica(self, replica, add_block_replicas = True):
        self._dataset_replicas[replica.dataset] = replica

        if add_block_replicas:
            for partition, site_partition in self.partitions.iteritems():
                block_replicas = set()
                for block_replica in replica.block_replicas:
                    if partition.contains(block_replica):
                        block_replicas.add(block_replica)
    
                if len(block_replicas) == 0:
                    continue
    
                if block_replicas == replica.block_replicas:
                    site_partition.replicas[replica] = None
                else:
                    site_partition.replicas[replica] = block_replicas

    def add_block_replica(self, replica):
        # this function should be called automatically to avoid integrity errors
        try:
            dataset_replica = self._dataset_replicas[replica.block.dataset]
        except KeyError:
            raise ObjectError('Dataset %s is not at %s' % (replica.block.dataset.name, self._name))

        if replica not in dataset_replica.block_replicas:
            raise IntegrityError('%s is not a block replica of %s' % (str(replica), str(dataset_replica)))

        for partition, site_partition in self.partitions.iteritems():
            if not partition.contains(replica):
                continue

            try:
                block_replica_list = site_partition.replicas[dataset_replica]
            except KeyError:
                if len(dataset_replica.block_replicas) == 1:
                    # this is the sole block replica
                    site_partition.replicas[dataset_replica] = None
                else:
                    site_partition.replicas[dataset_replica] = set([replica])
            else:
                if block_replica_list is None:
                    # assume this function was called for all new block replicas
                    # then we are just adding another replica to this partition
                    pass
                else:
                    # again assuming this function is called for all new block replicas,
                    # block_replica_list not being None implies that adding this new
                    # replica will not make the dataset replica in this partition complete
                    block_replica_list.add(replica)

    def update_partitioning(self, replica):
        if type(replica).__name__ == 'DatasetReplica':
            if replica not in self._dataset_replicas:
                return

            for partition, site_partition in self.partitions.iteritems():
                try:
                    block_replicas = site_partition.replicas[replica]
                except KeyError:
                    block_replicas = set()
    
                if block_replicas is None:
                    # previously, was all contained - need to check again
                    block_replicas = set()
                    for block_replica in replica.block_replicas:
                        if partition.contains(block_replica):
                            block_replicas.add(block_replica)

                    if block_replicas != replica.block_replicas:
                        site_partition.replicas[replica] = block_replicas

                    continue

                # remove block replicas that were deleted
                block_replicas &= replica.block_replicas

                # reevaluate existing block replicas
                for block_replica in list(block_replicas):
                    if not partition.contains(block_replica):
                        block_replicas.remove(block_replica)

                # add new block replicas
                new_replicas = replica.block_replicas - block_replicas
                for block_replica in new_replicas:
                    if partition.contains(block_replica):
                        block_replicas.add(block_replica)
               
                if len(block_replicas) == 0:
                    try:
                        site_partition.replicas.pop(replica)
                    except KeyError:
                        pass
                elif block_replicas == replica.block_replicas:
                    site_partition.replicas[replica] = None
                else:
                    site_partition.replicas[replica] = block_replicas

        else:
            # BlockReplica
            dataset_replica = replica.block.dataset.find_replica(replica.site)
            if dataset_replica not in self._dataset_replicas:
                # has to exist if you can find the replica from dataset
                # so maybe raise here?
                return

            for partition, site_partition in self.partitions.iteritems():
                try:
                    block_replicas = site_partition.replicas[dataset_replica]
                except KeyError:
                    block_replicas = set()

                if partition.contains(replica):
                    if block_replicas is None or replica in block_replicas:
                        # already included
                        continue
                    else:
                        block_replicas.add(replica)
                else:
                    if block_replicas is None:
                        # this dataset replica used to be fully included but now it's not
                        block_replicas = set(dataset_replica.block_replicas)
                        block_replicas.remove(replica)
                    elif replica in block_replicas:
                        block_replicas.remove(replica)

                if len(block_replicas) == 0:
                    try:
                        site_partition.replicas.pop(dataset_replica)
                    except KeyError:
                        pass
                elif block_replicas == dataset_replica.block_replicas:
                    site_partition.replicas[dataset_replica] = None
                else:
                    site_partition.replicas[dataset_replica] = block_replicas

    def remove_dataset_replica(self, replica):
        for site_partition in self.partitions.itervalues():
            try:
                site_partition.replicas.pop(replica)
            except KeyError:
                pass

        self._dataset_replicas.pop(replica.dataset)

    def remove_block_replica(self, replica, local = True):
        # local: Are we deleting the replica only for this site (BlockReplica object itself survives)?
        dataset_replica = self._dataset_replicas[replica.block.dataset]
        self._remove_block_replica(replica, dataset_replica, local)

    def _remove_block_replica(self, replica, dataset_replica, local):
        for site_partition in self.partitions.itervalues():
            try:
                block_replicas = site_partition.replicas[dataset_replica]
            except KeyError:
                continue

            if block_replicas is None:
                if local:
                    block_replicas = site_partition.replicas[dataset_replica] = set(dataset_replica.block_replicas)
                else:
                    # The site contained all block replicas. It will contain all after a global deletion.
                    continue

            block_replicas.remove(replica)

            if len(block_replicas) == 0:
                site_partition.replicas.pop(dataset_replica)
