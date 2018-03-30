import logging
import re

from dynamo.policy.condition import Condition
from dynamo.policy.variables import replica_variables
import dynamo.dataformat as df
import dynamo.core.impl as persistency_impl

LOG = logging.getLogger(__name__)

class NameKeyDict(dict):
    __slots__ = []

    def add(self, obj):
        self[obj.name] = obj


class ObjectRepository(object):
    """Base class of the inventory which is just a bundle of dicts"""
    def __init__(self):
        self.groups = NameKeyDict()
        self.sites = NameKeyDict()
        self.datasets = NameKeyDict()
        self.partitions = NameKeyDict()

        # null group always exist
        self.groups[None] = df.Group.null_group

    def load(self):
        pass

    def make_object(self, repstr):
        return eval('df.' + repstr)

    def update(self, obj):
        return obj.embed_into(self)

    def register_update(self, obj):
        pass

    def delete(self, obj):
        try:
            return obj.unlink_from(self)
        except (KeyError, df.ObjectError) as e:
            # When delete is attempted on a nonexistent object or something linked to a nonexistent object
            # As this is less alarming, error message is suppressed to debug level.
            LOG.debug('%s in inventory.delete(%s)', type(e).__name__, str(obj))
            # But we'll still raise - it's up to the users to trap this exception.
            raise
        except:
            LOG.error('Exception in inventory.delete(%s)' % str(obj))
            raise

    def clear_update(self):
        pass


class DynamoInventory(ObjectRepository):
    """
    Inventory class. ObjectRepository with a persistent store backend.
    """

    CMD_UPDATE, CMD_DELETE, CMD_EOM = range(3)
    _cmd_str = ['UPDATE', 'DELETE', 'EOM']

    def __init__(self, config, load = True):
        ObjectRepository.__init__(self)

        self.init_store(config.persistency.module, config.persistency.config)

        self.partition_def_path = config.partition_def_path
        
        if load:
            self.load()

        # When the user application is authorized to change the inventory state, all updated
        # and deleted objects are kept in this list until the end of execution.
        self._update_commands = None

    def init_store(self, module, config):
        persistency_cls = getattr(persistency_impl, module)
        self._store = persistency_cls(config)

        df.Block._inventory_store = self._store

    def load(self, groups = (None, None), sites = (None, None), datasets = (None, None)):
        """
        Load inventory content from persistency store.
        @param groups   2-tuple (included, excluded)
        @param sites    2-tuple (included, excluded)
        @param datasets 2-tuple (included, excluded)
        """
        
        self.groups.clear()
        self.groups[None] = df.Group.null_group
        self.sites.clear()
        self.datasets.clear()
        self.partitions.clear()

        LOG.info('Setting up partitions.')

        self._load_partitions()

        LOG.info('Loading data from local persistent storage.')

        group_names = self._get_group_names(*groups)
        site_names = self._get_site_names(*sites)
        dataset_names = self._get_dataset_names(*datasets)

        self._store.load_data(
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
        """Load partition data from a text table."""

        partition_names = self._store.get_partition_names()

        with open(self.partition_def_path) as defsource:
            subpartitions = {}
            for line in defsource:
                matches = re.match('([^:]+): *(.+)', line.strip())
                if matches is None:
                    continue
        
                name = matches.group(1)
                try:
                    partition_names.remove(name)
                except ValueError:
                    LOG.warning('Unknown partition %s appears in %s', name, self.partition_def_path)
                    continue

                condition_text = matches.group(2).strip()
                
                matches = re.match('\[(.+)\]$', condition_text)
                if matches:
                    partition = df.Partition(name)
                    subpartitions[partition] = map(str.strip, matches.group(1).split(','))
                else:
                    partition = df.Partition(name, Condition(condition_text, replica_variables))

                self.partitions.add(partition)

        if len(partition_names) != 0:
            LOG.error('No definition given for the following partitions: %s', partition_names)
            raise df.ConfigurationError()

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
            group_names = self._store.get_group_names(include = lists[0], exclude = lists[1])
            LOG.debug('Group names %s', group_names)
        else:
            group_names = None

        return group_names

    def _get_site_names(self, included, excluded):
        """Return the list of site names or None according to the arguments."""

        lists = self._parse_include_lists(included, excluded)

        if lists is not None:
            site_names = self._store.get_site_names(include = lists[0], exclude = lists[1])
            LOG.debug('Site names %s', site_names)
        else:
            site_names = None

        return site_names

    def _get_dataset_names(self, included, excluded):
        """Return the list of dataset names or None according to the arguments."""

        lists = self._parse_include_lists(included, excluded)

        if lists is not None:
            dataset_names = self._store.get_dataset_names(include = lists[0], exclude = lists[1])
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

    def update(self, obj, write = False, changelog = None):
        """
        Update an object. Only update the member values of the immediate object.
        When calling from a subprocess, pass down the unlinked clone of the argument
        to _update_commands.
        @param obj    Object to embed into this inventory.
        @param write  Write updated object to persistent store.
        """

        try:
            embedded_clone, updated = obj.embed_into(self, check = True)
        except:
            LOG.error('Exception in inventory.update(%s)', str(obj))
            raise

        if updated:
            self.register_update(embedded_clone, write, changelog)

        return embedded_clone

    def register_update(self, obj, write = False, changelog = None):
        """
        Put the obj to _update_commands list and write to store.
        """

        if self._update_commands is not None:
            if changelog is not None:
                changelog.info('Updating %s', str(obj))

            LOG.debug('%s has changed. Adding a clone to updated objects list.', str(obj))
            # The content of update_commands gets pickled and shipped back to the server process.
            # Pickling process follows all links between the objects. We create an unlinked clone
            # here to avoid shipping the entire inventory.
            self._update_commands.append((DynamoInventory.CMD_UPDATE, obj.unlinked_clone()))

        if write:
            if changelog is not None:
                changelog.info('Saving %s', str(obj))

            LOG.debug('%s has changed. Saving changes to inventory store.', str(obj))
            try:
                obj.write_into(self._store)
            except:
                LOG.error('Exception writing %s to inventory store', str(obj))
                raise

    def delete(self, obj, write = False):
        """
        Delete an object. Behavior over other objects linked to the one deleted
        depends on the type.
        @param obj    Object to delete from this inventory.
        @param write  Record deletion to persistent store.
        """

        deleted_object = ObjectRepository.delete(self, obj)

        if deleted_object is None:
            return

        if self._update_commands is not None:
            self._update_commands.append((DynamoInventory.CMD_DELETE, deleted_object.unlinked_clone(attrs = False)))

        if write:
            try:
                deleted_object.delete_from(self._store)
            except:
                LOG.error('Exception writing deletion of %s to inventory store', str(obj))
                raise

    def clear_update(self):
        """
        Empty the _updated_objects and _deleted_objects lists. This operation
        *does not* revert the updates done to this inventory.
        """

        self._update_commands = []
