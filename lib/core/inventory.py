import logging
import re

from dynamo.policy.condition import Condition
from dynamo.policy.variables import replica_variables
import dynamo.dataformat as df
from dynamo.core.components.persistency import InventoryStore

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

        # Null group always exist
        self.groups[None] = df.Group.null_group

        # This base class does not actually have a persistency store
        self._store = None

    def update(self, obj):
        return obj.embed_into(self)

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

    def make_object(self, repstr):
        """
        Create an object from its representation string.

        @param repstr  A string returned by repr(obj)

        @return A new object represented by the input.
        """

        return eval('df.' + repstr)

    def find_file(self, lfn):
        """
        There is no rule for mapping the file name to blocks and datasets. This function
        uses the persistency store (which would be a relational database for any sane installation)
        to retrieve a fully-linked file object given an LFN.

        @param lfn   Logical name of the file to find

        @return A fully-linked File object
        """

        result = self._store.find_block_containing(lfn)
        # result is None or (dataset_name, block_name)

        if result is None:
            return None

        # Not capturing exceptions because there shouldn't be anything raised
        # If there is, that means we have a really bad desynch between the inventory and the store.
        return self.datasets[result[0]].find_block(result[1], must_find = True).find_file(lfn, must_find = True)


class DynamoInventoryProxy(ObjectRepository):
    """Inventory object used by Dynamo applications"""
    def __init__(self, inventory):
        self.groups = inventory.groups
        self.sites = inventory.sites
        self.datasets = inventory.datasets
        self.partitions = inventory.partitions
        self._store = inventory.new_store_handle()

        # When the user application is authorized to change the inventory state, all updated
        # and deleted objects are kept in this list until the end of execution.
        self._update_commands = None

    def update(self, obj): #override
        """
        Update an object. Only update the member values of the immediate object.
        If object state changes, register the new clone to be sent back to the Dynamo server.
        @param obj    Object to embed into this inventory.
        """

        try:
            embedded_clone, updated = obj.embed_into(self, check = True)
        except:
            LOG.error('Exception in inventory.update(%s)', str(obj))
            raise

        if updated:
            self.register_update(embedded_clone)

        return embedded_clone

    def register_update(self, obj): #override
        """
        Put the text representation of obj to _update_commands.
        """

        if self._update_commands is None:
            return

        LOG.debug('%s has changed. Adding a clone to updated objects list.', str(obj))
        self._update_commands.append((DynamoInventory.CMD_UPDATE, repr(obj)))

    def delete(self, obj): #override
        """
        Delete an object. Behavior over other objects linked to the one deleted
        depends on the type.
        @param obj    Object to delete from this inventory.
        """

        deleted_object = ObjectRepository.delete(self, obj)

        if deleted_object is None:
            return None

        if self._update_commands is not None:
            LOG.debug('%s is deleted.', str(obj))
            self._update_commands.append((DynamoInventory.CMD_DELETE, repr(deleted_object)))

        return deleted_object

    def clear_update(self): #override
        """
        Empty the _updated_objects and _deleted_objects lists. This operation
        *does not* revert the updates done to this inventory.
        """

        if self._update_commands is not None:
            self._update_commands = []


class DynamoInventory(ObjectRepository):
    """
    Inventory class. ObjectRepository with a persistent store backend.
    """

    CMD_UPDATE, CMD_DELETE, CMD_EOM = range(3)
    _cmd_str = ['UPDATE', 'DELETE', 'EOM']

    @property
    def has_store(self):
        return self._has_store

    def __init__(self, config):
        ObjectRepository.__init__(self)

        # True when full content is available in memory
        self.loaded = False

        # True if configured with a PersistencyStore this can write to
        # (all inventories will have a persistency backend, but cannot write to dynamically assigned ones)
        self._has_store = False

        self._store = None
        if 'persistency' in config:
            self._has_store = True
            self.init_store(config.persistency.module, config.persistency.config)

        self.partition_def_path = config.partition_def_path

    def init_store(self, module, config):
        if self._store:
            self._store.close()

        self._store = InventoryStore.get_instance(module, config)

        df.Block._inventory_store = self._store

    def clone_store(self, module, config):
        source = InventoryStore.get_instance(module, config)
        self._store.clone_from(source)
        source.close()

    def store_version(self):
        return self._store.version()

    def check_store(self):
        """
        Check the connection to store.
        """
        return self._store.check_connection()

    def flush_to_store(self):
        """
        Save the full inventory content to store.
        """
        self._store.save_data(self)

    def new_store_handle(self):
        return self._store.new_handle()

    def create_proxy(self):
        return DynamoInventoryProxy(self)

    def load(self, groups = (None, None), sites = (None, None), datasets = (None, None)):
        """
        Load inventory content from persistency store.
        @param groups   2-tuple (included, excluded)
        @param sites    2-tuple (included, excluded)
        @param datasets 2-tuple (included, excluded)
        """

        self.loaded = False
        
        self.groups.clear()
        self.groups[None] = df.Group.null_group
        self.sites.clear()
        self.datasets.clear()
        self.partitions.clear()

        LOG.info('Setting up partitions.')

        self._load_partitions()

        LOG.info('Loading data from persistent storage.')

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

        self.loaded = True

    def _load_partitions(self):
        """Load partition data from a text table."""

        conditions = {}
        with open(self.partition_def_path) as defsource:
            subpartitions = {}
            for line in defsource:
                matches = re.match('([^:]+): *(.+)', line.strip())
                if matches is None:
                    continue
        
                name = matches.group(1)
                condition_text = matches.group(2).strip()

                matches = re.match('\[(.+)\]$', condition_text)
                if matches:
                    condition = map(str.strip, matches.group(1).split(','))
                else:
                    condition = Condition(condition_text, replica_variables)

                conditions[name] = condition

        partitions = self._store.get_partitions(conditions)

        for partition in partitions:
            self.partitions.add(partition)

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

    def update(self, obj): #override
        """
        Update an object in memory and write it to store.
        @param obj        Object to embed into this inventory.
        """

        LOG.debug('Saving changes on %s to inventory store.', str(obj))

        embedded_clone = ObjectRepository.update(self, obj)

        if self._has_store:
            try:
                embedded_clone.write_into(self._store)
            except:
                LOG.error('Exception writing %s to inventory store', str(obj))
                raise

        return embedded_clone

    def delete(self, obj): #override
        """
        Delete an object from memory and write the change to store.
        depends on the type.
        @param obj    Object to delete from this inventory.
        """

        deleted_object = ObjectRepository.delete(self, obj)

        if deleted_object is None:
            return None

        if self._has_store:
            try:
                deleted_object.delete_from(self._store)
            except:
                LOG.error('Exception writing deletion of %s to inventory store', str(obj))
                raise

        return deleted_object
