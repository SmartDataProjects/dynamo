import logging
import fnmatch
import random

from dynamo.dataformat import Site, BlockReplica

LOG = logging.getLogger(__name__)

class ReplicaPlacementRule(object):
    """
    Defining the interface for replica placement rules.
    """

    def __init__(self):
        pass

    def dataset_allowed(self, dataset, site):
        return True

    def block_allowed(self, block, site):
        return True


class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, config):
        self.partition_name = config.partition_name
        self.group_name = config.group_name

        self.target_site_names = list(config.target_sites)
        # Do not copy data to sites beyond target occupancy fraction (0-1)
        self.target_site_occupancy = config.target_site_occupancy
        # Maximum fraction of the quota that can be pending at a single site.
        self.max_site_pending_fraction = config.max_site_pending_fraction
        # Maximum overall volume that can be queued in this cycle for transfer.
        # The value is given in TB in the configuration file.
        self.max_total_cycle_volume = config.max_total_cycle_volume * 1.e+12

        self.placement_rules = []

        # To be set at runtime
        self.target_sites = set()

    def set_target_sites(self, sites, partition):
        """
        @param sites   List of Site objects
        """

        for site in sites:
            if self.is_target_site(site.partitions[partition]):
                self.target_sites.add(site)

    def is_target_site(self, site_partition, additional_volume = 0.):
        site = site_partition.site
        quota = site_partition.quota

        if site.status != Site.STAT_READY:
            LOG.debug('%s is not ready', site.name)
            return False

        matches = False
        for pattern in self.target_site_names:
            if pattern.startswith('!'):
                if fnmatch.fnmatch(site.name, pattern[1:]):
                    matches = False
            else:
                if fnmatch.fnmatch(site.name, pattern):
                    matches = True

        if not matches:
            LOG.debug('%s does not match target site def', site.name)
            return False

        if self.target_site_occupancy < 1.:
            if quota == 0.:
                LOG.debug('%s has no quota', site.name)
                return False
            elif quota > 0.:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(additional_volume) / quota
        
                if occupancy_fraction > self.target_site_occupancy:
                    LOG.debug('%s occupancy fraction %f > %f', site.name, occupancy_fraction, self.target_site_occupancy)
                    return False

        if self.max_site_pending_fraction < 1.:
            if quota == 0.:
                LOG.debug('%s has no quota', site.name)
                return False
            elif quota > 0.:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(additional_volume) / quota

                # Difference between projected and physical volumes
                pending_fraction = occupancy_fraction
                pending_fraction -= site_partition.occupancy_fraction(physical = True)
        
                if pending_fraction > self.max_site_pending_fraction:
                    LOG.debug('%s pending fraction %f > %f', site.name, pending_fraction, self.max_site_pending_fraction)
                    return False

        return True

    def is_allowed_destination(self, request, site):
        """
        Check if the request item is allowed to be at site, according to the set of rules.
        """

        for rule in self.placement_rules:
            if request.block is not None:
                if not rule.block_allowed(request.block, site):
                    return False
            elif request.blocks is not None:
                for block in request.blocks:
                    if not rule.block_allowed(block, site):
                        return False
            else:
                if not rule.dataset_allowed(request.dataset, site):
                    return False

        return True

    def validate_source(self, request):
        if request.blocks is not None:
            for block in request.blocks:
                for replica in block.replicas:
                    if replica.is_complete():
                        break
                else:
                    # no block complete
                    if BlockReplica._use_file_ids:
                        # can determine completion at file level
                        block_files = set(f.id for f in block.files)
                        replica_files = set()
                        for replica in block.replicas:
                            if replica.file_ids is None:
                                # can't happen but hey
                                replica_files = block_files
                                break
                            else:
                                replica_files.update(replica.file_ids)

                        if block_files != replica_files:
                            # some files missing
                            return False
                    else:
                        return False

        elif request.block is not None:
            for replica in request.block.replicas:
                if replica.is_complete():
                    break
            else:
                # no block complete
                if BlockReplica._use_file_ids:
                    # can determine completion at file level
                    block_files = set(f.id for f in request.block.files)
                    replica_files = set()
                    for replica in request.block.replicas:
                        if replica.file_ids is None:
                            # can't happen but hey
                            replica_files = block_files
                            break
                        else:
                            replica_files.update(replica.file_ids)

                    if block_files != replica_files:
                        # some files missing
                        return False
                else:
                    return False

        else:
            replica_blocks = set()
            for replica in request.dataset.replicas:
                if replica.is_complete():
                    return True

                for block_replica in replica.block_replicas:
                    if block_replica.is_complete():
                        replica_blocks.add(block_replica.block)

            if request.dataset.blocks == replica_blocks:
                return True

            if BlockReplica._use_file_ids:
                # some blocks missing - go to file level
                dataset_files = set(f.id for f in request.dataset.files)
                replica_files = set()
                for replica in request.dataset.replicas:
                    for block_replica in replica.block_replicas:
                        replica_files.update(f.id for f in block_replica.files())
    
                if dataset_files != replica_files:
                    return False
            else:
                return False

        return True

    def find_destination_for(self, request, partition, candidates = None):
        if candidates is None:
            candidates = self.target_sites

        item_size = request.item_size()

        site_array = []
        for site in candidates:
            site_partition = site.partitions[partition]

            # replica must not be at the site already
            if request.item_already_exists(site) != 0:
                continue

            # placement must be allowed by the policy
            if not self.is_allowed_destination(request, site):
                continue

            p = 1.

            if site_partition.quota > 0.:
                projected_occupancy = site_partition.occupancy_fraction(physical = False)
                projected_occupancy += float(item_size) / site_partition.quota
    
                # total projected volume must not exceed the quota
                if projected_occupancy > 1.:
                    continue

                p -= projected_occupancy

            if len(site_array) != 0:
                p += site_array[-1][1]

            site_array.append((site, p))

        if len(site_array) == 0:
            LOG.warning('%s has no copy destination.', request.item_name())
            return 'No destination available'

        x = random.uniform(0., site_array[-1][1])

        isite = next(k for k in range(len(site_array)) if x < site_array[k][1])

        request.destination = site_array[isite][0]

        return None

    def check_destination(self, request, partition):
        if request.destination not in self.target_sites:
            LOG.debug('Destination %s for %s is not a target site.', request.destination.name, request.item_name())
            return 'Not a target site'

        if not self.is_allowed_destination(request, request.destination):
            LOG.debug('Placement of %s to %s not allowed by policy.', request.item_name(), request.destination.name)
            return 'Not allowed'

        exists_level = request.item_already_exists()

        if exists_level == 2: # exists and owned by the same group
            LOG.debug('%s is already at %s.', request.item_name(), request.destination.name)
            return 'Replica exists'

        elif exists_level == 0: # does not exist
            site_partition = request.destination.partitions[partition]
            if site_partition.quota > 0:
                occupancy_fraction = site_partition.occupancy_fraction(physical = False)
                occupancy_fraction += float(request.item_size()) / site_partition.quota
            else:
                occupancy_fraction = 1.
    
            if occupancy_fraction >= 1.:
                LOG.debug('Cannot copy %s to %s because destination is full.', request.item_name(), request.destination.name)
                return 'Destination is full'

        return None
