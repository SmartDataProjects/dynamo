from common.dataformat import Dataset, Block, Site, IntegrityError

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


    CLEAR_NONE = 0
    CLEAR_REPLICAS = 1
    CLEAR_ALL = 2

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

    def release_lock(self, force = False):
        if self._lock_depth == 1 or force:
            self._do_release_lock()

        self._lock_depth -= 1

    def make_snapshot(self, clear = CLEAR_NONE):
        """
        Make a snapshot of the current state of the persistent inventory. Flag clear = True
        will "move" the data into the snapshot, rather than cloning it.
        """

        self.acquire_lock()
        try:
            self._do_make_snapshot(clear)
        finally:
            self.release_lock()

    def prepare_new(self):
        self.acquire_lock()
        try:
            self._do_prepare_new()
        finally:
            self.release_lock()

    def load_data(self):
        """
        Return dictionaries {site_name: site}, {group name: group}, {dataset_name: dataset}
        loaded from persistent storage.
        """

        self.acquire_lock()
        try:
            site_list, group_list, dataset_list = self._do_load_data()
        finally:
            self.release_lock()

        return site_list, group_list, dataset_list

    def save_data(self, site_list, group_list, dataset_list):
        """
        Write information in the dictionaries into persistent storage.
        Remove information of datasets and blocks with no replicas.
        """

        self.acquire_lock()
        try:
            self._do_save_data(site_list, group_list, dataset_list)
            self._do_clean_block_info()
            self._do_clean_dataset_info()
        finally:
            self.release_lock()


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Inventory interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'InventoryInterface class to be used.')

    args = parser.parse_args()

    command = args.command[0]
    cmd_args = args.command[1:]

    if args.class_name == '':
        interface = classes.default_interface['inventory']()
    else:
        interface = getattr(classes, args.class_name)()

    if command == 'snapshot':
        clear = InventoryInterface.CLEAR_NONE
        if len(cmd_args) > 1 and cmd_args[0] == 'clear':
            if cmd_args[1] == 'replicas':
                clear = InventoryInterface.CLEAR_REPLICAS
            elif cmd_args[2] == 'all':
                clear = InventoryInterface.CLEAR_ALL

        interface.make_snapshot(clear = clear)

    else:
        sites, groups, datasets = interface.load_data()

        if command == 'datasets':
            print datasets.keys()

        elif command == 'groups':
            print groups.keys()

        elif command == 'sites':
            print sites.keys()
