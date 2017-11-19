import logging
import re

from common.configuration import common_config
from policy.condition import Condition
from policy.variables import replica_variables
from dataformat import Partition
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

    def load(self, groups = (None, None), sites = (None, None), datasets = (None, None)):
        """
        Load inventory content from persistency store.
        @param groups   2-tuple (included, excluded)
        @param sites    2-tuple (included, excluded)
        @param datasets 2-tuple (included, excluded)
        """
        
        self.groups.clear()
        self.sites.clear()
        self.datasets.clear()
        self.partitions.clear()

        LOG.info('Setting up partitions.')

        self._load_partitions()

        LOG.info('Loading data from local persistent storage.')

        group_names = self._get_group_names(*groups)
        site_names = self._get_site_names(*sites)
        dataset_names = self._get_dataset_names(*datasets)

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

    def _load_partitions(self):
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

                self.partitions.add(partition)

        for partition, subp_names in subpartitions.iteritems():
            try:
                subparts = tuple(self.partitions[name] for name in subp_names)
            except KeyError:
                raise IntegrityError('Unknown partition ' + name + ' specified in subpartition list for ' + partition.name)

            partition._subpartitions = subparts
            for subp in subparts:
                subp._parent = partition

    def _get_group_names(self, included, excluded):
        """Return the list of group names or None according to the arguments."""

        lists = self._parse_include_lists(included, excluded)

        if lists is not None:
            group_names = self.store.get_group_names(include = lists[0], exclude = lists[1])
            LOG.debug('Group names %s', group_names)
        else:
            group_names = None

        return group_names

    def _get_site_names(self, included, excluded):
        """Return the list of site names or None according to the arguments."""

        lists = self._parse_include_lists(included, excluded)

        if lists is not None:
            site_names = self.store.get_site_names(include = lists[0], exclude = lists[1])
            LOG.debug('Site names %s', site_names)
        else:
            site_names = None

        return site_names

    def _get_dataset_names(self, included, excluded):
        """Return the list of dataset names or None according to the arguments."""

        lists = self._parse_include_lists(included, excluded)

        if lists is not None:
            dataset_names = self.store.get_dataset_names(include = lists[0], exclude = lists[1])
            LOG.debug('Dataset names %s', dataset_names)
        else:
            dataset_names = None

        return dataset_names

    def _parse_include_lists(self, included, excluded):
        """
        Simple subroutine to convert include and exclude lists into a standard format.
        @param included  A str or list of name patterns of included objects
        @param excluded  A str or list of name patterns of excluded objects
        @return  (include_list, exclude_list) or None
        """

        has_spec = False

        if included is not None:
            has_spec = True

            if type(included) is list:
                include_list = included
            else:
                include_list = [included]
        else:
            include_list = ['*']

        if excluded is not None:
            has_spec = True

            if type(excluded) is list:
                exclude_list = excluded
            else:
                exclude_list = [excluded]
        else:
            exclude_list = []

        if has_spec:
            return include_list, exclude_list
        else:
            return None

    def update(self, obj, check = False, write = False):
        """
        Update an object. Only update the member values of the immediate object.
        When calling from a subprocess, pass an unlinked copy to _updated_objects.
        @param obj    Object to embed into this inventory.
        @param check  Passed to obj.embed_into(). Check equivalency of obj to existing object first.
        @param write  Write updated object to persistent store.
        """

        updated = obj.embed_into(self, check = check)

        if updated:
            if hasattr(self, '_updated_objects'):
                self._updated_objects.append(obj.unlinked_clone())
    
            if write:
                # do something with self.store
                pass

    def delete(self, obj, write = False):
        """
        Delete an object. Behavior over other objects linked to the one deleted
        depends on the type.
        @param obj    Object to delete from this inventory.
        @param write  Record deletion to persistent store.
        """

        obj.delete_from(self)
        
        if hasattr(self, '_deleted_objects'):
            self._deleted_objects.append(obj.unlinked_clone())

        if write:
            # do something with self.store
            pass
