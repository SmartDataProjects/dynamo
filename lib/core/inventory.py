import logging
import re

from common.configuration import common_config
from policy.condition import Condition
from policy.variables import replica_variables
from dataformat import *
import core.impl

LOG = logging.getLogger(__name__)

class DynamoInventory(object):
    def __init__(self):
        persistency_cls = getattr(core.impl, common_config.inventory.persistency.module)
        self.store = persistency_cls(common_config.inventory.persistency.config)

        self.groups = {}
        self.sites = {}
        self.datasets = {}
        self.partitions = {}

        self.load()

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
            group_names = []
       
        site_names = self.store.get_site_names(include = common_config.inventory.included_sites, exclude = common_config.inventory.excluded_sites)

        LOG.debug('Site names %s', site_names)

        if common_config.debug.included_datasets != '*':
            dataset_names = self.store.get_dataset_names(include = [common_config.debug.included_datasets])
            LOG.debug('Dataset names %s', dataset_names)
        else:
            dataset_names = []

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

    def update(self, obj):
        """
        Update an object. Only update the member values of the immediate object.
        When calling from a subprocess, pass an unlinked copy to _updated_objects.
        """
        
        tp = type(obj)

        if tp is Group:
            try:
                group = self.groups[obj.name]
            except KeyError:
                obj.linked_clone(self)
            else:
                group.copy(obj)

        elif tp is Partition:
            try:
                partition = self.partitions[obj.name]
            except KeyError:
                obj.linked_clone(self)
            else:
                partition.copy(obj)

        elif tp is Site:
            try:
                site = self.sites[obj.name]
            except KeyError:
                obj.linked_clone(self)
            else:
                site.copy(obj)

        elif tp is SitePartition:
            try:
                site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            try:
                partition = self.partitions[obj.partition.name]
            except KeyError:
                 raise ObjectError('Unknown partition %s', obj.partition.name)

            site_partition = site.partitions[partition]
            site_partition.copy(obj)

        elif tp is Dataset:
            try:
                dataset = self.datasets[obj.name]
            except KeyError:
                obj.linked_clone(self)
            else:
                dataset.copy(obj)

        elif tp is Block:
            try:
                dataset = self.datasets[obj.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.dataset.name)

            block = dataset.find_block(obj.name)
            if block is None:
                obj.linked_clone(self)
            else:
                block.copy(obj)

        elif tp is File:
            try:
                dataset = self.datasets[obj.block.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.block.dataset.name)

            block = dataset.find_block(obj.block.name, must_find = True)

            lfile = block.find_file(obj.fullpath())
            if lfile is None:
                obj.linked_clone(self)
            else:
                lfile.copy(obj)

        elif tp is DatasetReplica:
            try:
                dataset = self.datasets[obj.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.dataset.name)

            try:
                site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            replica = dataset.find_replica(site)
            if replica is None:
                obj.linked_clone(self)
            else:
                replica.copy(obj)

        elif tp is BlockReplica:
            try:
                dataset = self.datasets[obj.block.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.block.dataset.name)

            block = dataset.find_block(obj.block.name, must_find = True)

            try:
                site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            replica = block.find_replica(site)
            if replica is None:
                obj.linked_clone(self)
            else:
                replica.copy(obj)

        else:
            return

        if hasattr(self, '_updated_objects'):
            self._updated_objects.append(obj.unlinked_clone())

    def delete(self, obj):
        """
        Delete an object. Behavior over other objects linked to the one deleted
        depends on the type.
        """
        
        tp = type(obj)

        if tp is Group:
            # Pop the group from the main list. All block replicas owned by the group
            # will be disowned.
            group = self.groups.pop(obj.name)

            for dataset in self.datasets.itervalues():
                for replica in dataset.replicas:
                    for block_replica in replica.block_replicas:
                        if block_replica.group == group:
                            block_replica.group = None

        elif tp is Partition:
            # Pop the partition from the main list, and remove site_partitions.
            partition = self.partitions.pop(obj.name)

            for site in self.sites.itervalues():
                site.partitions.pop(partition)

        elif tp is Site:
            # Pop the site from the main list, and remove all replicas on the site.
            site = self.sites.pop(obj.name)

            for dataset in self.datasets.itervalues():
                for replica in list(dataset.replicas):
                    if replica.site == site:
                        dataset.replicas.remove(replica)
                        for block_replica in replica.block_replicas:
                            block_replica.block.replicas.remove(block_replica)

        elif tp is SitePartition:
            raise ObjectError('Deleting a single SitePartition is not allowed.')

        elif tp is Dataset:
            # Pop the dataset from the main list, and remove all replicas.
            dataset = self.datasets.pop(obj.name)

            for replica in dataset.replicas:
                replica.site.remove_dataset_replica(replica)

        elif tp is Block:
            # Remove the block from the dataset, and remove all replicas.
            dataset = self.datasets[obj.dataset.name]
            block = dataset.find_block(obj.name, must_find = True)
            dataset.remove_block(block)
            
            for replica in block.replicas:
                replica.site.remove_block_replica(replica)

        elif tp is File:
            dataset = self.datasets[obj.block.dataset.name]
            block = dataset.find_block(obj.block.name)
            lfile = block.find_file(obj.fullpath())
            block.remove_file(lfile)

        elif tp is DatasetReplica:
            dataset = self.datasets[obj.dataset.name]
            site = self.sites[obj.site.name]
            replica = site.find_dataset_replica(dataset)

            dataset.replicas.remove(replica)
            for block_replica in replica.block_replicas:
                block_replica.block.replicas.remove(block_replica)

            site.remove_dataset_replica(replica)

        elif tp is BlockReplica:
            dataset = self.datasets[obj.block.dataset.name]
            block = dataset.find_block(obj.block.name, must_find = True)
            site = self.sites[obj.site.name]
            dataset_replica = site.find_dataset_replica(dataset)
            replica = block.find_replica(site)

            dataset_replica.block_replicas.remove(replica)
            block.replicas.remove(replica)
            site.remove_block_replica(replica)

        else:
            return

        if hasattr(self, '_deleted_objects'):
            self._deleted_objects.append(obj.unlinked_clone())
