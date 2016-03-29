import logging
import time

from common.dataformat import Dataset, Block, Site, IntegrityError

logger = logging.getLogger(__name__)

class InventoryInterface(object):
    """
    Interface to local inventory database.
    """

    class LockError(Exception):
        pass

    CLEAR_NONE = 0
    CLEAR_REPLICAS = 1
    CLEAR_ALL = 2

    def __init__(self):
        # Allow multiple calls to acquire-release. No other process can acquire
        # the lock until the depth in this process is 0.
        self._lock_depth = 0

        self.last_update = 0
        self.debug_mode = False

    def acquire_lock(self):
        if self._lock_depth == 0:
            self._do_acquire_lock()

        self._lock_depth += 1

    def release_lock(self, force = False):
        if self._lock_depth == 1 or force:
            self._do_release_lock()

        if self._lock_depth > 0: # should always be the case if properly programmed
            self._lock_depth -= 1

    def make_snapshot(self, clear = CLEAR_NONE):
        """
        Make a snapshot of the current state of the persistent inventory. Flag clear = True
        will "move" the data into the snapshot, rather than cloning it.
        """

        if self.debug_mode:
            logger.debug('_do_make_snapshot(%d)', clear)
            return

        self.acquire_lock()
        try:
            self._do_make_snapshot(clear)
        finally:
            self.release_lock()

    def remove_snapshot(self, newer_than = 0, older_than = 0):
        if older_than == 0:
            older_than = time.time()

        if self.debug_mode:
            logger.debug('_do_remove_snapshot(%f, %f)', newer_than, older_than)
            return

        self.acquire_lock()
        try:
            self._do_remove_snapshot(newer_than, older_than)
        finally:
            self.release_lock()

    def list_snapshots(self):
        """
        List the timestamps of the inventory snapshots that is not the current.
        """

        return self._do_list_snapshots()

    def switch_snapshot(self, timestamp):
        """
        Switch the data source to an existing snapshot.
        """

        if timestamp not in self.list_snapshots():
            print 'Cannot switch to snapshot', timestamp
            return

        while self._lock_depth > 0:
            self.release_lock()

        self._do_switch_snapshot(timestamp)

    def load_data(self, site_filt = '*', dataset_filt = '/*/*/*'):
        """
        Return lists loaded from persistent storage.
        """

        if self.debug_mode:
            logger.debug('_do_load_data()')

        self.acquire_lock()
        try:
            site_list, group_list, dataset_list = self._do_load_data(site_filt, dataset_filt)
        finally:
            self.release_lock()

        return site_list, group_list, dataset_list

    def save_data(self, sites, groups, datasets, clean_stale = True):
        """
        Write information in the dictionaries into persistent storage.
        Remove information of datasets and blocks with no replicas.
        Return newly inserted sites, groups, and datasets.
        Arguments are name->obj maps.
        Optional argument clean_stale specifies if stale data (e.g. information
        of nonexistent datasets) should be cleaned.
        """

        if self.debug_mode:
            logger.debug('_do_save_data()')
            return

        self.acquire_lock()
        try:
            self._do_save_data(sites, groups, datasets, clean_stale)
        finally:
            self.release_lock()

    def delete_dataset(self, dataset):
        """
        Delete dataset from persistent storage.
        """

        if self.debug_mode:
            logger.debug('_do_delete_dataset(%s)', dataset.name)
            return

        self.acquire_lock()
        try:
            self._do_delete_dataset(dataset)
        finally:
            self.release_lock()

    def delete_block(self, block):
        """
        Delete block from persistent storage.
        """

        if self.debug_mode:
            logger.debug('_do_delete_block(%s)', block.name)
            return

        self.acquire_lock()
        try:
            self._do_delete_block(block)
        finally:
            self.release_lock()

    def delete_datasetreplica(self, replica):
        """
        Delete dataset replica from persistent storage.
        """

        if self.debug_mode:
            logger.debug('_do_delete_datasetreplica(%s:%s)', replica.site.name, replica.dataset.name)
            return

        self.acquire_lock()
        try:
            self._do_delete_datasetreplica(replica)
        finally:
            self.release_lock()

    def delete_blockreplica(self, replica):
        """
        Delete block replica from persistent storage.
        """

        if self.debug_mode:
            logger.debug('_do_delete_blockreplica(%s:%s)', replica.site.name, replica.block.name)
            return

        self.acquire_lock()
        try:
            self._do_delete_blockreplica(replica)
        finally:
            self.release_lock()

    def close_block(self, block):
        """
        Set and save block.is_open = False
        """

        if type(block) is Block:
            dataset_name = block.dataset.name
            block_name = block.name
        elif type(block) is str:
            dataset_name, sep, block_name = block.partition('#')

        if self.debug_mode:
            logger.debug('_do_close_block(%s#%s)', dataset_name, block_name)
            return

        self.acquire_lock()
        try:
            self._do_close_block(dataset_name, block_name)
        finally:
            self.release_lock()

    def set_dataset_status(self, dataset, status):
        """
        Set and save dataset status
        """

        # convert status into a string
        status_str = Dataset.status_name(status)

        if type(dataset) is Dataset:
            dataset_name = dataset.name
        elif type(dataset) is str:
            dataset_name = dataset

        if self.debug_mode:
            logger.debug('_do_set_dataset_status(%s, %s)', dataset.name, status_str)
            return

        self.acquire_lock()
        try:
            self._do_set_dataset_status(dataset_name, status_str)
        finally:
            self.release_lock()


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Inventory interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(snapshot [clear (replicas|all)]|clean|list (datasets|groups|sites))')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'InventoryInterface class to be used.')
    parser.add_argument('--timestamp', '-t', metavar = 'YMDHMS', dest = 'timestamp', default = '', help = 'Timestamp of the snapshot to be loaded / cleaned. With command clean, prepend with "<" or ">" to remove all snapshots older or newer than the timestamp.')

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
            elif cmd_args[1] == 'all':
                clear = InventoryInterface.CLEAR_ALL

        if args.timestamp:
            interface.switch_snapshot(args.timestamp)

        interface.make_snapshot(clear = clear)

    elif command == 'clean':
        if not args.timestamp:
            newer_than = 0
            older_than = time.time()
        elif args.timestamp.startswith('>'):
            newer_than = time.mktime(time.strptime(args.timestamp[1:], '%y%m%d%H%M%S'))
            older_than = time.time()
        elif args.timestamp.startswith('<'):
            newer_than = 0
            older_than = time.mktime(time.strptime(args.timestamp[1:], '%y%m%d%H%M%S'))
        else:
            newer_than = time.mktime(time.strptime(args.timestamp, '%y%m%d%H%M%S'))
            older_than = newer_than

        interface.remove_snapshot(newer_than = newer_than, older_than = older_than)

    elif command == 'list':
        if args.timestamp:
            interface.switch_snapshot(args.timestamp)

        if cmd_args[0] != 'snapshots':
            sites, groups, datasets = interface.load_data()
    
            if cmd_args[0] == 'datasets':
                print [d.name for d in datasets]
    
            elif cmd_args[0] == 'groups':
                print [g.name for g in groups]
    
            elif cmd_args[0] == 'sites':
                print [s.name for s in sites]

        else:
            print interface.list_snapshots()

    elif command == 'show':
        if args.timestamp:
            interface.switch_snapshot(args.timestamp)

        if cmd_args[0] == 'dataset':
            sites, groups, datasets = interface.load_data(dataset_filt = cmd_args[1])

            try:
                dataset = next(d for d in datasets if d.name == cmd_args[1])
            except StopIteration:
                print 'No dataset %s found.' % cmd_args[1]
                sys.exit(0)

            print dataset

        elif cmd_args[0] == 'block':
            dataset_name, sep, block_name = cmd_args[1].partition('#')
            sites, groups, datasets = interface.load_data(dataset_filt = dataset_name)

            try:
                dataset = next(d for d in datasets if d.name == dataset_name)
            except StopIteration:
                print 'No dataset %s found.' % dataset
                sys.exit(0)

            try:
                block = next(b for b in dataset.blocks if b.name == block_name)
            except StopIteration:
                print 'No block %s found in dataset %s.' % (block_name, dataset.name)
                sys.exit(0)

            print block

#        elif cmd_args[0] == 'site':
#            sites, groups, datasets = interface.load_data(site_filt = cmd_args[1])
#
#            try:
#                site = next(s for s in sites if s.name == cmd_args[1])
#            except StopIteration:
#                print 'No site %s found.' % cmd_args[1]
#                sys.exit(0)
#
#            print site

    elif command == 'close_block':
        interface.close_block(cmd_args[0])

    elif command == 'set_dataset_status':
        interface.set_dataset_status(cmd_args[0], cmd_args[1])
