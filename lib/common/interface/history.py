import logging
import time

from common.dataformat import HistoryRecord
import common.configuration as config

logger = logging.getLogger(__name__)

class TransactionHistoryInterface(object):
    """
    Interface for transaction history. Has a locking mechanism similar to store.
    """

    def __init__(self):
        self._lock_depth = 0

    def acquire_lock(self):
        if self._lock_depth == 0:
            self._do_acquire_lock()

        self._lock_depth += 1

    def release_lock(self, force = False):
        if self._lock_depth == 1 or force:
            self._do_release_lock()

        if self._lock_depth > 0: # should always be the case if properly programmed
            self._lock_depth -= 1

    def make_snapshot(self):
        """
        Make a snapshot of the current state of the persistent records.
        """

        timestamp = time.strftime('%y%m%d%H%M%S')

        if config.read_only:
            logger.debug('_do_make_snapshot(%s, %d)', timestamp)
            return

        self.acquire_lock()
        try:
            self._do_make_snapshot(timestamp)
        finally:
            self.release_lock()

    def remove_snapshot(self, newer_than = 0, older_than = 0):
        if older_than == 0:
            older_than = time.time()

        if config.read_only:
            logger.debug('_do_remove_snapshot(%f, %f)', newer_than, older_than)
            return

        self.acquire_lock()
        try:
            self._do_remove_snapshot(newer_than, older_than)
        finally:
            self.release_lock()

    def list_snapshots(self):
        """
        List the timestamps of the snapshots that is not the current.
        """

        return self._do_list_snapshots()

    def recover_from(self, timestamp):
        """
        Recover records from a snapshot (current content will be lost!)
        timestamp can be 'last'.
        """

        timestamps = self.list_snapshots()

        if len(timestamps) == 0:
            print 'No snapshots taken.'
            return

        if timestamp == 'last':
            timestamp = timestamps[0]
            print 'Recovering history records from snapshot', timestamp
            
        elif timestamp not in timestamps:
            print 'Cannot copy from snapshot', timestamp
            return

        while self._lock_depth > 0:
            self.release_lock()

        self._do_recover_from(timestamp)

    def new_copy_run(self, partition, is_test = False):
        """
        Set up a new copy/deletion run for the partition.
        """

        if config.read_only:
            logger.info('new_run')
            return 0

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_COPY, partition, is_test)
        finally:
            self.release_lock()

        return run_number

    def new_deletion_run(self, partition, is_test = False):
        """
        Set up a new copy/deletion run for the partition.
        """

        if config.read_only:
            logger.info('new_run')
            return 0

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_DELETE, partition, is_test)
        finally:
            self.release_lock()

        return run_number

    def close_copy_run(self, run_number):
        if config.read_only:
            logger.info('close_copy_run')
            return

        self.acquire_lock()
        try:
            self._do_close_run(HistoryRecord.OP_COPY, run_number)
        finally:
            self.release_lock()

    def close_deletion_run(self, run_number):
        if config.read_only:
            logger.info('close_copy_run')
            return

        self.acquire_lock()
        try:
            self._do_close_run(HistoryRecord.OP_DELETE, run_number)
        finally:
            self.release_lock()

    def make_copy_entry(self, run_number, site, operation_id, approved, dataset_list, size):
        if config.read_only:
            logger.info('make_copy_entry')
            return

        self.acquire_lock()
        try:
            if operation_id < 0:
                operation_id = self.get_next_test_id()

            self._do_make_copy_entry(run_number, site, operation_id, approved, dataset_list, size)
        finally:
            self.release_lock()

    def make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size):
        if config.read_only:
            logger.info('make_deletion_entry')
            return

        self.acquire_lock()
        try:
            if operation_id < 0:
                operation_id = self.get_next_test_id()

            self._do_make_deletion_entry(run_number, site, operation_id, approved, datasets, size)
        finally:
            self.release_lock()

    def update_copy_entry(self, copy_record):
        """
        Update copy entry from the argument. Only certain fields (approved, last_update) are updatable.
        """

        if config.read_only:
            logger.info('update_copy_entry')
            return

        self.acquire_lock()
        try:
            self._do_update_copy_entry(copy_record)
        finally:
            self.release_lock()

    def update_deletion_entry(self, deletion_record):
        """
        Update deletion entry from the argument. Only certain fields (approved, last_update) are updatable.
        """

        if config.read_only:
            logger.info('update_deletion_entry')
            return

        self.acquire_lock()
        try:
            self._do_update_deletion_entry(deletion_record)
        finally:
            self.release_lock()

    def save_sites(self, run_number, inventory):
        """
        Save sites that are in the inventory but not in the history records.
        """

        if config.read_only:
            logger.info('save_sites')
            return

        self.acquire_lock()
        try:
            self._do_save_sites(run_number, inventory)
        finally:
            self.release_lock()

    def save_datasets(self, run_number, inventory):
        """
        Save datasets that are in the inventory but not in the history records.
        """

        if config.read_only:
            logger.info('save_datasets')
            return

        self.acquire_lock()
        try:
            self._do_save_datasets(run_number, inventory)
        finally:
            self.release_lock()

    def save_quotas(self, run_number, partition, quotas, inventory):
        """
        Update quota snapshots.
        """

        if config.read_only:
            logger.info('save_quotas')
            return

        self.acquire_lock()
        try:
            self._do_save_quotas(run_number, partition, quotas, inventory)
        finally:
            self.release_lock()

    def save_replicas(self, run_number, inventory):
        """
        Update replica snapshots.
        """

        if config.read_only:
            logger.info('save_replicas')
            return

        self.acquire_lock()
        try:
            self._do_save_replicas(run_number, inventory)
        finally:
            self.release_lock()

    def save_copy_decisions(self, run_number, copies):
        """
        Save reasons for copy decisions? Still deciding what to do..
        """

        if config.read_only:
            logger.info('save_copy_decisions')
            return

        self.acquire_lock()
        try:
            self._do_save_copy_decisions(run_number, copies)
        finally:
            self.release_lock()
      
    def save_deletion_decisions(self, run_number, protected, deleted, kept):
        """
        Save decisions and their reasons for all replicas.
        Arguments protected, deleted, and kept are all dict {replica: reason}
        """

        if config.read_only:
            logger.info('save_deletion_decisions')
            return

        self.acquire_lock()
        try:
            self._do_save_deletion_decisions(run_number, protected, deleted, kept)
        finally:
            self.release_lock()

    def get_incomplete_copies(self, partition):
        self.acquire_lock()
        try:
            # list of HistoryRecords
            copies = self._do_get_incomplete_copies(partition)
        finally:
            self.release_lock()

        return copies

    def get_incomplete_deletions(self):
        self.acquire_lock()
        try:
            # list of HistoryRecords
            deletions = self._do_get_incomplete_deletions()
        finally:
            self.release_lock()

        return deletions

    def get_site_name(self, operation_id):
        self.acquire_lock()
        try:
            site_name = self._do_get_site_name(operation_id)
        finally:
            self.release_lock()

        return site_name

    def get_next_test_id(self):
        self.acquire_lock()
        try:
            test_id = self._do_get_next_test_id()
        finally:
            self.release_lock()

        return test_id


if __name__ == '__main__':

    import sys
    import time
    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Local inventory store interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = '(update|check {copy|deletion} <operation_id>)')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'TransactionHistoryInterface class to be used.')
    parser.add_argument('--copy-class', '-p', metavar = 'CLASS', dest = 'copy_class_name', default = '', help = 'CopyInterface class to be used.')
    parser.add_argument('--deletion-class', '-d', metavar = 'CLASS', dest = 'deletion_class_name', default = '', help = 'DeletionInterface class to be used.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')

    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    if args.class_name == '':
        interface = classes.default_interface['history']()
    else:
        interface = getattr(classes, args.class_name)()

    if args.copy_class_name == '':
        copy_interface = classes.default_interface['copy']()
    else:
        copy_interface = getattr(classes, args.copy_class_name)()

    if args.deletion_class_name == '':
        deletion_interface = classes.default_interface['deletion']()
    else:
        deletion_interface = getattr(classes, args.deletion_class_name)()

    icmd = 0
    while icmd != len(args.command):
        command = args.command[icmd]
        icmd += 1

        if command == 'update':
            incomplete_copies = interface.get_incomplete_copies()
            
            for record in incomplete_copies:
                updates = copy_interface.copy_status(record.operation_id)

                last_update = max([last_update for last_update, total, copied in updates.values()])
                if last_update > record.last_update:
                    logger.info('Updating record for copy %d to %s.', record.operation_id, record.site_name)
            
                    record.last_update = last_update
                    record.done = sum(copied for last_update, total, copied in updates.values())
                    interface.update_copy_entry(record)
            
            incomplete_deletions = interface.get_incomplete_deletions()
            
            for record in incomplete_deletions:
                updates = deletion_interface.deletion_status(record.operation_id)

                last_update = max([last_update for last_update, total, deleted in updates.values()])
                if last_update > record.last_update:
                    logger.info('Updating record for deletion %d at %s.', record.operation_id, record.site_name)
            
                    record.last_update = last_update
                    record.done = sum(deleted for last_update, total, deleted in updates.values())
                    interface.update_deletion_entry(record)
    
        elif command == 'check':
            operation = args.command[icmd]
            icmd += 1

            try:
                operation_ids = [int(args.command[icmd])]
                icmd += 1
            except:
                operations = interface.get_incomplete_copies()
                operation_ids = [op.operation_id for op in operations]

            for operation_id in operation_ids:
                print 'ID: %d' % operation_id
                print 'Site: ' + interface.get_site_name(operation_id)
    
                if operation == 'copy':
                    status = copy_interface.copy_status(operation_id)
    
                    total = 0.
                    done = 0.
                    latest_update = 0
                    for dataset in sorted(status.keys()):
                        size, copied, last_update = status[dataset]
                        if size == 0: # why??
                            print '{dataset} (0 GB) [{update}]'.format(dataset = dataset, update = time.ctime(last_update))
                        else:
                            print '{dataset} ({total:.2f} GB): {percentage:.2f}% [{update}]'.format(dataset = dataset, total = size * 1.e-9, percentage = float(copied) / size * 100., update = time.ctime(last_update))

                        total += size
                        done += copied
                        if last_update > latest_update:
                            latest_update = last_update
    
                    print '----------------------------'
                    if total == 0: # why??
                        print 'Transfer NAN% complete'
                    else:
                        print 'Transfer {percentage:.2f}% complete [{update}]'.format(percentage = done / total * 100., update = time.ctime(latest_update))

                    print ''

        elif command == 'snapshot':
            interface.make_snapshot()

        elif command == 'recover':
            timestamp = args.command[icmd]
            icmd += 1

            interface.recover_from(timestamp)
