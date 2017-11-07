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
                my_obj = self.groups[obj.name]
            except KeyError:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is Partition:
            try:
                my_obj = self.partitions[obj.name]
            except KeyError:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is Site:
            try:
                my_obj = self.sites[obj.name]
            except KeyError:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is SitePartition:
            try:
                my_site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            try:
                my_partition = self.partitions[obj.partition.name]
            except KeyError:
                 raise ObjectError('Unknown partition %s', obj.partition.name)

            my_obj = my_site.partitions[my_partition]
            my_obj.copy(obj)

        elif tp is Dataset:
            try:
                my_obj = self.datasets[obj.name]
            except KeyError:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is Block:
            try:
                my_dataset = self.datasets[obj.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.dataset.name)

            my_obj = my_dataset.find_block(obj.name)
            if my_obj is None:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is File:
            try:
                my_dataset = self.datasets[obj.block.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.block.dataset.name)

            my_block = my_dataset.find_block(obj.block.name, must_find = True)

            my_obj = my_block.find_file(obj.fullpath())
            if my_obj is None:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is DatasetReplica:
            try:
                my_dataset = self.datasets[obj.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.dataset.name)

            try:
                my_site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            my_obj = my_dataset.find_replica(my_site)
            if my_obj is None:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        elif tp is BlockReplica:
            try:
                my_dataset = self.datasets[obj.block.dataset.name]
            except KeyError:
                raise ObjectError('Unknown dataset %s', obj.block.dataset.name)

            my_block = my_dataset.find_block(obj.block.name, must_find = True)

            try:
                my_site = self.sites[obj.site.name]
            except KeyError:
                raise ObjectError('Unknown site %s', obj.site.name)

            my_obj = my_block.find_replica(my_site)
            if my_obj is None:
                my_obj = obj.linked_clone(self)
            else:
                my_obj.copy(obj)

        else:
            return

        if hasattr(self, '_updated_objects'):
            self._updated_objects.append(my_obj.unlinked_clone())
