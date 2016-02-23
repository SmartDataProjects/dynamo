from common.dataformat import Dataset, Block, Site, IntegrityError

class StatusProbeInterface(object):
    """
    Interface specs for probe to the system status. Collects information from
    available sources and parse it in a form that can be stored in the inventory
    database.
    """

    def __init__(self):
        pass

    def get_site_info(self, name = '*'):
        """Return a list of sites that match the wildcard name."""
        return []

    def get_dataset_info(self, name = '/*/*/*'):
        """Return a list of datasets that match the wildcard name."""
        return []


class TransferInterface(object):
    """
    Interface to data transfer application.
    """

    def __init__(self):
        pass

    def schedule_copy(self, dataset, origin, dest):
        pass


class DeletionInterface(object):
    """
    Interface to data deletion application.
    """

    def __init__(self):
        pass

    def schedule_deletion(self, obj, site):
        pass


class InventoryInterface(object):
    """
    Interface to local inventory database.
    """

    class LockError(Exception):
        pass

    class Cache(object):
        def __init__(self, obj):
            self.obj = obj
            self.stale = False

        def update(self, **keywords):
            for key, value in keywords.items():
                setattr(self.obj, key, value)
                
            self.stale = False


    def __init__(self):
        # Allow multiple calls to acquire-release. No other process can acquire
        # the lock until the depth in this process is 0.
        self._lock_depth = 0

        self._dataset_cache = {}
        self._block_cache = {}
        self._site_cache = {}

    def acquire_lock(self):
        if self._lock_depth == 0:
            self._do_acquire_lock()

        self._lock_depth += 1

    def release_lock(self):
        if self._lock_depth == 1:
            self._do_release_lock()

        self._lock_depth -= 1

    def make_snapshot(self):
        self.acquire_lock()
        self._do_make_snapshot()
        self.release_lock()

    def prepare_new(self):
        self.acquire_lock()
        self._do_prepare_new()
        self.release_lock()

    def update_dataset_list(self, datasets):
        self._update_list(datasets, Dataset)

    def update_block_list(self, blocks):
        self._update_list(blocks, Block)

    def update_site_list(self, sites):
        self._update_list(sites, Site)

    def create_dataset_info(self, dataset):
        self.acquire_lock()
        self._do_create_dataset_info(obj)
        self.release_lock()

    def update_dataset_info(self, dataset):
        self.acquire_lock()
        self._do_update_dataset_info(dataset)
        self.release_lock()

    def delete_dataset_info(self, name):
        self.acquire_lock()
        self._do_delete_dataset_info(name)
        self.release_lock()

    def create_block_info(self, block):
        self.acquire_lock()        
        self._do_create_block_info(block)
        self.release_lock()        

    def update_block_info(self, block):
        self.acquire_lock()
        self._do_update_block_info(block)
        self.release_lock()

    def delete_block_info(self, name):
        self.acquire_lock()
        self._do_delete_block_info(name)
        self.release_lock()

    def create_site_info(self, site):
        self.acquire_lock()        
        self._do_create_site_info(obj)
        self.release_lock()        

    def update_site_info(self, site):
        self.acquire_lock()
        self._do_update_site_info(site)
        self.release_lock()

    def delete_site_info(self, name):
        self.acquire_lock()
        self._do_delete_site_info(name)
        self.release_lock()

    def place_dataset(self, replicas):
        self.acquire_lock()
        for replica in replicas:
            self._do_place_dataset(replica)
        self.release_lock()

    def place_block(self, replicas):
        self.acquire_lock()
        for replica in replicas:
            self._do_place_block(replica)
        self.release_lock()

    def _update_list(self, objs, obj_type):
        if len(objs) == 0:
            return

        self.acquire_lock()

        if obj_type is Dataset:
            known_names = set(self._get_known_dataset_names())
            delete = self._do_delete_dataset_info_list
            create = self._do_create_dataset_info_list
            update = self._do_update_dataset_info_list

        elif obj_type is Block:
            known_names = set(self._get_known_block_names())
            delete = self._do_delete_block_info_list
            create = self._do_create_block_info_list
            update = self._do_update_block_info_list

        elif obj_type is Site:
            known_names = set(self._get_known_site_names())
            delete = self._do_delete_site_info_list
            create = self._do_create_site_info_list
            update = self._do_update_site_info_list

        else:
            raise RuntimeError('Update requested for a list of unknown type ' + str(obj_type))

        input_names = set([o.name for o in objs])

        deleted_names = known_names - input_names
        delete(deleted_names)

        new_names = input_names - known_names
        create([o for o in objs if o.name in new_names])

        old_names = known_names - new_names - deleted_names
        update([o for o in objs if o.name in old_names])

        # now fill cache

        self.release_lock()

    def _do_acquire_lock(self):
        pass

    def _do_release_lock(self):
        pass

    def _do_make_snapshot(self):
        pass

    def _do_prepare_new(self):
        pass

    def _get_known_dataset_names(self):
        return []

    def _get_known_block_names(self):
        return []

    def _get_known_site_names(self):
        return []

    def _do_create_dataset_info(self, dataset):
        return 0

    def _do_update_dataset_info(self, dataset):
        pass

    def _do_delete_dataset_info(self, dataset):
        pass

    def _do_create_dataset_info_list(self, datasets):
        """Derived classes can implement more efficient list operations"""

        for dataset in datasets:
            self.create_dataset_info(dataset)

    def _do_update_dataset_info_list(self, datasets):
        """Derived classes can implement more efficient list operations"""

        for dataset in datasets:
            self.update_dataset_info(dataset)

    def _do_delete_dataset_info_list(self, names):
        """Derived classes can implement more efficient list operations"""

        for name in names:
            self.delete_dataset_info(name)

    def _do_create_block_info(self, block):
        return 0

    def _do_update_block_info(self, block):
        pass

    def _do_delete_block_info(self, block):
        pass

    def _do_create_block_info_list(self, blocks):
        """Derived classes can implement more efficient list operations"""

        for block in blocks:
            self.create_block_info(block)

    def _do_update_block_info_list(self, blocks):
        """Derived classes can implement more efficient list operations"""

        for block in blocks:
            self.update_block_info(block)

    def _do_delete_block_info_list(self, names):
        """Derived classes can implement more efficient list operations"""

        for name in names:
            self.delete_block_info(name)

    def _do_create_site_info(self, site):
        return 0

    def _do_update_site_info(self, site):
        pass

    def _do_delete_site_info(self, site):
        pass

    def _do_create_site_info_list(self, sites):
        """Derived classes can implement more efficient list operations"""

        for site in sites:
            self.create_site_info(site)

    def _do_update_site_info_list(self, sites):
        """Derived classes can implement more efficient list operations"""

        for site in sites:
            self.update_site_info(site)

    def _do_delete_site_info_list(self, names):
        """Derived classes can implement more efficient list operations"""

        for name in names:
            self.delete_site_info(name)

    def _do_place_dataset(self, dataset_replica):
        pass

    def _do_place_block(self, block_replica):
        pass
