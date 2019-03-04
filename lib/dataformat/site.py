import copy
import re

from exceptions import ObjectError, IntegrityError
from sitepartition import SitePartition

class Site(object):
    """Represents a site. Owns lists of dataset and block replicas, which are organized into partitions."""

    __slots__ = ['_name', 'id', 'host', 'storage_type', 'backend', 'status', 'filename_mapping',
                 '_dataset_replicas', 'partitions', 'x509proxy']

    _storage_types = ['disk', 'mss', 'buffer', 'unknown']
    TYPE_DISK, TYPE_MSS, TYPE_BUFFER, TYPE_UNKNOWN = range(1, len(_storage_types) + 1)
    _statuses = ['ready', 'waitroom', 'morgue', 'unknown']
    STAT_READY, STAT_WAITROOM, STAT_MORGUE, STAT_UNKNOWN = range(1, len(_statuses) + 1)

    @staticmethod
    def storage_type_val(arg):
        try:
            return eval('Site.TYPE_' + arg.upper())
        except:
            return arg

    @staticmethod
    def storage_type_name(arg):
        try:
            return Site._storage_types[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('Site.STAT_' + arg.upper())
        except:
            return arg

    @staticmethod
    def status_name(arg):
        try:
            return Site._statuses[arg - 1]
        except:
            return arg

    @property
    def name(self):
        return self._name

    class FileNameMapping(object):
        def __init__(self, chains):
            """
            @param chains  List of chains. A chain is a list of 2-tuples (lfn pattern, pfn replacement)
                           PFN replacement can contain {n} placeholders to match the captured re patterns
                           (starting with n = 0).
            """
            # remember the original texts for comparison
            self._chains = copy.deepcopy(chains)
            # compiled versions for actual use
            self._re_chains = []
            for chain in chains:
                re_chain = []
                for lfnpat, pfnpat in chain:
                    re_chain.append((re.compile(lfnpat), pfnpat))

                self._re_chains.append(re_chain)

        def __eq__(self, other):
            return self._chains == other._chains

        def __ne__(self, other):
            return self._chains != other._chains

        def __repr__(self):
            return repr(self._chains)

        def map(self, lfn):
            for chain in self._re_chains:
                source = lfn
                for source_re, dest_pat in chain:
                    matches = source_re.match(source)
                    if matches is None:
                        break

                    source = dest_pat.format(*tuple(matches.group(i + 1) for i in xrange(source_re.groups)))
                else:
                    # could go through the entire chain - source is the mapped pfn
                    return source

            return None


    def __init__(self, name, host = '', storage_type = TYPE_DISK, backend = '', status = STAT_UNKNOWN, filename_mapping = {}, x509proxy = None, sid = 0):
        self._name = name
        self.host = host
        self.storage_type = Site.storage_type_val(storage_type)
        self.backend = backend
        self.status = Site.status_val(status)

        self.filename_mapping = {}
        for protocol, chains in filename_mapping.iteritems():
            self.filename_mapping[protocol] = Site.FileNameMapping(chains)

        self.id = sid

        self._dataset_replicas = {} # {Dataset: DatasetReplica}

        self.partitions = {} # {Partition: SitePartition}

        self.x509proxy = x509proxy

    def __str__(self):
        return 'Site %s (host=%s, storage_type=%s, backend=%s, status=%s, x509=%s, id=%d)' % \
            (self._name, self.host, Site.storage_type_name(self.storage_type), self.backend, Site.status_name(self.status), self.x509proxy, self.id)

    def __repr__(self):
        return 'Site(%s,%s,\'%s\',%s,\'%s\',%s,%s,%d)' % \
            (repr(self._name), repr(self.host), Site.storage_type_name(self.storage_type), repr(self.backend), Site.status_name(self.status), repr(self.filename_mapping), repr(self.x509proxy), self.id)

    def __eq__(self, other):
        return self is other or \
            (self._name == other._name and self.host == other.host and self.storage_type == other.storage_type and \
             self.backend == other.backend and self.status == other.status and \
             self.filename_mapping == other.filename_mapping and self.x509proxy == other.x509proxy)

    def __ne__(self, other):
        return not self.__eq__(other)

    def copy(self, other):
        """Only copy simple member variables."""

        self.host = other.host
        self.storage_type = other.storage_type
        self.backend = other.backend
        self.status = other.status
        self.filename_mapping = {}
        for protocol, mapping in other.filename_mapping.iteritems():
            self.filename_mapping[protocol] = Site.FileNameMapping(mapping._chains)

        self.x509proxy = other.x509proxy

    def embed_into(self, inventory, check = False):
        updated = False

        try:
            site = inventory.sites[self._name]
        except KeyError:
            site = Site(self._name)
            site.copy(self)
            inventory.sites.add(site)

            for partition in inventory.partitions.itervalues():
                site.partitions[partition] = SitePartition(site, partition)

            updated = True

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

    def unlink_from(self, inventory):
        try:
            site = inventory.sites.pop(self._name)
        except KeyError:
            return None

        for replica in site._dataset_replicas.values():
            replica.unlink()

        for partition in site.partitions.keys():
            site.partitions.pop(partition)

        return site

    def write_into(self, store):
        store.save_site(self)
        # if a new site, store must create SitePartition entries with default values

    def delete_from(self, store):
        store.delete_site(self)

    def find_dataset_replica(self, dataset, must_find = False):
        try:
            return self._dataset_replicas[dataset]
        except KeyError:
            if must_find:
                raise ObjectError('Could not find replica of %s in %s' % (dataset.name, self._name))
            else:
                return None

    def find_block_replica(self, block, must_find = False):
        if type(block).__name__ == 'Block':
            try:
                dataset_replica = self._dataset_replicas[block.dataset]
            except KeyError:
                if must_find:
                    raise ObjectError('Could not find replica of %s in %s' % (block.dataset.name, self._name))
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
                raise ObjectError('Could not find replica of %s in %s' % (block.full_name(), self._name))
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
        if replica.site is not self:
            raise ObjectError('%s passed to update_partitioning of %s' % (str(replica), str(self)))

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
            dataset_replica = self.find_dataset_replica(replica.block.dataset)

            if dataset_replica is None:
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
                        # make a copy of the full list of block replicas
                        block_replicas = set(dataset_replica.block_replicas)
                        block_replicas.remove(replica)
                    else:
                        try:
                            block_replicas.remove(replica)
                        except KeyError:
                            # not included already
                            pass

                if len(block_replicas) == 0:
                    try:
                        site_partition.replicas.pop(dataset_replica)
                    except KeyError:
                        pass

                elif block_replicas == dataset_replica.block_replicas:
                    site_partition.replicas[dataset_replica] = None
                else:
                    site_partition.replicas[dataset_replica] = block_replicas

    def to_pfn(self, lfn, protocol):
        try:
            mapping = self.filename_mapping[protocol]
        except KeyError:
            return None

        return mapping.map(lfn)
