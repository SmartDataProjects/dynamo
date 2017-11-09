import logging
import re

from common.configuration import common_config
from policy.condition import Condition
from policy.variables import replica_variables
from dataformat import *
import core.impl

LOG = logging.getLogger(__name__)

class NameKeyDict(dict):
    __slots__ = []

    def add(self, obj):
        self[obj.name] = obj

class DynamoInventory(object):
    def __init__(self, persistency_config = None, load = True):
        self.groups = NameKeyDict()
        self.sites = NameKeyDict()
        self.datasets = NameKeyDict()
        self.partitions = NameKeyDict()

        self.init_store(persistency_config)

        if load:
            self.load()

    def init_store(self, config = None):
        persistency_cls = getattr(core.impl, common_config.inventory.persistency.module)

        if config is not None:
            # can be privileged store instance
            self.store = persistency_cls(config)
        else:
            # unprivileged read-only store instance
            self.store = persistency_cls(common_config.inventory.persistency.config)

    def load(self):
        self.groups.clear()
        self.sites.clear()
        self.datasets.clear()
        self.partitions.clear()

        LOG.info('Setting up partitions.')

        self.load_partitions()

        LOG.info('Loading data from local persistent storage.')

        if common_config.debug.included_groups != '*':
            group_names = self.store.get_group_names(include = [common_config.debug.included_groups])
            LOG.debug('Group names %s', group_names)
        else:
            group_names = None
       
        site_names = self.store.get_site_names(include = common_config.inventory.included_sites, exclude = common_config.inventory.excluded_sites)

        LOG.debug('Site names %s', site_names)

        if common_config.debug.included_datasets != '*':
            dataset_names = self.store.get_dataset_names(include = [common_config.debug.included_datasets])
            LOG.debug('Dataset names %s', dataset_names)
        else:
            dataset_names = None

        self.store.load_data(
            self,
            group_names = group_names,
            site_names = site_names,
            dataset_names = dataset_names
        )

        num_dataset_replicas = 0
        num_block_replicas = 0

        for dataset in self.datasets.itervalues():
            num_dataset_replicas += len(dataset.replicas)
            num_block_replicas += sum(len(r.block_replicas) for r in dataset.replicas)

        LOG.info('Data is loaded to memory. %d groups, %d sites, %d datasets, %d dataset replicas, %d block replicas.\n', len(self.groups), len(self.sites), len(self.datasets), num_dataset_replicas, num_block_replicas)

    def load_partitions(self):
        with open(common_config.general.paths.base + '/policies/partitions.txt') as defsource:
            subpartitions = {}
            for line in defsource:
                matches = re.match('([^:]+): *(.+)', line.strip())
                if matches is None:
                    continue
        
                name = matches.group(1)
                condition_text = matches.group(2).strip()

                matches = re.match('\[(.+)\]$', condition_text)
                if matches:
                    partition = Partition(name)
                    subpartitions[partition] = map(str.strip, matches.group(1).split(','))
                else:
                    partition = Partition(name, Condition(condition_text, replica_variables))

                self.partitions[name] = partition

        for partition, subp_names in subpartitions.iteritems():
            try:
                subparts = tuple(self.partitions[name] for name in subp_names)
            except KeyError:
                raise IntegrityError('Unknown partition ' + name + ' specified in subpartition list for ' + partition.name)

            partition.subpartitions = subparts
            for subp in subparts:
                subp.parent = partition

    def add_dataset_to_site(self, dataset, site, group = None, blocks = None):
        """
        Create a new DatasetReplica object and return.
        """

        new_replica = DatasetReplica(dataset, site)

        dataset.replicas.add(new_replica)

        if blocks is None:
            # dataset.blocks cannot be None at this point
            blocks = dataset.blocks

        for block in blocks:
            block_replica = BlockReplica(block, site, group, is_complete = False, is_custodial = False, size = 0, last_update = 0)
            new_replica.block_replicas.add(block_replica)

        site.add_dataset_replica(new_replica)

        return new_replica

    def add_block_to_site(self, block, site, group = None):
        """
        Create a new BlockReplica object and return.
        """

        dataset = block.dataset

        dataset_replica = dataset.find_replica(site)

        if dataset_replica is None:
            dataset_replica = DatasetReplica(dataset, site)
    
            dataset.replicas.add(dataset_replica)
            site.add_dataset_replica(dataset_replica)

        new_replica = BlockReplica(block, site, group, is_complete = False, is_custodial = False, size = 0, last_update = 0)
        dataset_replica.block_replicas.add(new_replica)
        site.add_block_replica(new_replica)

        return new_replica

    def update(self, obj, write = False):
        """
        Update an object. Only update the member values of the immediate object.
        When calling from a subprocess, pass an unlinked copy to _updated_objects.
        """

        obj.embed_into(self)
        
        if hasattr(self, '_updated_objects'):
            self._updated_objects.append(obj.unlinked_clone())

        if write:
            # do something with self.store
            pass

    def delete(self, obj, write = False):
        """
        Delete an object. Behavior over other objects linked to the one deleted
        depends on the type.
        """

        obj.delete_from(self)
        
        if hasattr(self, '_deleted_objects'):
            self._deleted_objects.append(obj.unlinked_clone())

        if write:
            # do something with self.store
            pass
