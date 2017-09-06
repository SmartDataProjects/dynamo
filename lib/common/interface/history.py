import logging
import time

from common.dataformat import HistoryRecord
import common.configuration as config

logger = logging.getLogger(__name__)

class TransactionHistoryInterface(object):
    """
    Interface for transaction history. Has a locking mechanism similar to store.
    """

    class LockError(Exception):
        pass

    def __init__(self):
        self._lock_depth = 0

    def acquire_lock(self, blocking = True):
        if self._lock_depth == 0:
            locked = self._do_acquire_lock(blocking)
            if not locked: # only happens when not blocking
                return False

        self._lock_depth += 1
        return True

    def release_lock(self, force = False):
        if self._lock_depth == 1 or force:
            self._do_release_lock(force)

        if self._lock_depth > 0: # should always be the case if properly programmed
            self._lock_depth -= 1

    def make_snapshot(self, tag = ''):
        """
        Make a snapshot of the current state of the persistent records.
        """

        if not tag:
            tag = time.strftime('%y%m%d%H%M%S')

        if config.read_only:
            logger.debug('_do_make_snapshot(%s, %d)', tag)
            return

        self.acquire_lock()
        try:
            self._do_make_snapshot(tag)
        finally:
            self.release_lock()

    def remove_snapshot(self, tag = '', newer_than = 0, older_than = 0):
        if not tag and older_than == 0:
            older_than = time.time()

        if tag == 'last':
            tags = self.list_snapshots(timestamp_only = True)

            if len(tags) == 0:
                logger.warning('No snapshots taken.')
                return

            tag = tags[0]
            newer_than = 0
            older_than = 0

        if config.read_only:
            logger.debug('_do_remove_snapshot(%s, %f, %f)', tag, newer_than, older_than)
            return

        self.acquire_lock()
        try:
            self._do_remove_snapshot(tag, newer_than, older_than)
        finally:
            self.release_lock()

    def list_snapshots(self, timestamp_only = False):
        """
        List the tags of the snapshots that is not the current.
        """

        return self._do_list_snapshots(timestamp_only)

    def recover_from(self, tag):
        """
        Recover records from a snapshot (current content will be lost!)
        tag can be 'last'.
        """

        if tag == 'last':
            tags = self.list_snapshots(timestamp_only = True)
        else:
            tags = self.list_snapshots()

        if len(tags) == 0:
            logger.warning('No snapshots taken.')
            return

        if tag == 'last':
            tag = tags[0]
            logger.info('Recovering history records from snapshot %s', tag)
            
        elif tag not in tags:
            logger.error('Cannot copy from snapshot %s', tag)
            return

        while self._lock_depth > 0:
            self.release_lock()

        self._do_recover_from(tag)

    def new_copy_run(self, partition, policy_version, is_test = False, comment = ''):
        """
        Set up a new copy/deletion run for the partition.
        """

        if config.read_only:
            logger.info('new_run')
            return 0

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_COPY, partition, policy_version, is_test, comment)
        finally:
            self.release_lock()

        return run_number

    def new_deletion_run(self, partition, policy_version, is_test = False, comment = ''):
        """
        Set up a new copy/deletion run for the partition.
        """

        if config.read_only:
            logger.info('new_run')
            return 0

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_DELETE, partition, policy_version, is_test, comment)
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
            logger.info('close_deletion_run')
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

    def save_sites(self, run_number, sites):
        """
        Save status of sites.
        """

        if config.read_only:
            logger.info('save_sites')
            return

        self.acquire_lock()
        try:
            self._do_save_sites(run_number, sites)
        finally:
            self.release_lock()

    def get_sites(self, run_number = 0, partition = ''):
        """
        Collect the site status for a given run number or the latest run of the partition
        and return as a plain dict.
        """

        if run_number == 0:
            deletion_runs = self.get_deletion_runs(partition)
            copy_runs = self.get_copy_runs(partition)
            if len(deletion_runs) == 0 and len(copy_runs) == 0:
                raise RuntimeError('No history record exists')
            elif len(deletion_runs) == 0:
                run_number = copy_runs[0]
            elif len(copy_runs) == 0:
                run_number = deletion_runs[0]
            else:
                run_number = max(deletion_runs[0], copy_runs[0])

        self.acquire_lock()
        try:
            sites_info = self._do_get_sites(run_number)
        finally:
            self.release_lock()

        return sites_info

    def save_datasets(self, run_number, datasets):
        """
        Save datasets that are in the inventory but not in the history records.
        """

        if config.read_only:
            logger.info('save_datasets')
            return

        self.acquire_lock()
        try:
            self._do_save_datasets(run_number, datasets)
        finally:
            self.release_lock()

    def save_quotas(self, run_number, quotas):
        """
        Update quota snapshots.
        """

        if config.read_only:
            logger.info('save_quotas')
            return

        self.acquire_lock()
        try:
            self._do_save_quotas(run_number, quotas)
        finally:
            self.release_lock()

    def save_conditions(self, policies):
        """
        Save policy conditions.
        """

        if config.read_only:
            logger.info('save_conditions')
            return

        self.acquire_lock()
        try:
            self._do_save_conditions(policies)
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
      
    def save_deletion_decisions(self, run_number, deleted, kept, protected):
        """
        Save decisions and their reasons for all replicas.
        @param run_number Cycle number.
        @param deleted    {replica: condition or ([block_replica], condition)}
        @param kept       {replica: condition or ([block_replica], condition)}
        @param protected  {replica: condition or ([block_replica], condition)}

        Note that in case of block-level operations, one dataset replica can appear
        in multiple of deleted, kept, and protected.
        """

        if config.read_only:
            logger.info('save_deletion_decisions')
            return

        self.acquire_lock()
        try:
            self._do_save_deletion_decisions(run_number, deleted, kept, protected)
        finally:
            self.release_lock()

    def get_deletion_decisions(self, run_number, size_only = True):
        """
        Return a dict {site: (protect_size, delete_size, keep_size)} if size_only = True.
        Else return a massive dict {site: [(dataset, size, decision, reason)]}
        """

        self.acquire_lock()
        try:
            decisions = self._do_get_deletion_decisions(run_number, size_only)
        finally:
            self.release_lock()

        return decisions

    def save_dataset_popularity(self, run_number, datasets):
        """
        Second argument popularities is a list [(dataset, popularity_score)].
        """

        if config.read_only:
            logger.info('save_dataset_popularity')
            return

        self.acquire_lock()
        try:
            self._do_save_dataset_popularity(run_number, datasets)
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

    def get_copied_replicas(self, run_number):
        """
        Get the list of (site name, dataset name) copied in the given run.
        """
        self.acquire_lock()
        try:
            # list of HistoryRecords
            copies = self._do_get_copied_replicas(run_number)
        finally:
            self.release_lock()

        return copies

    def get_site_name(self, operation_id):
        self.acquire_lock()
        try:
            site_name = self._do_get_site_name(operation_id)
        finally:
            self.release_lock()

        return site_name

    def get_deletion_runs(self, partition, first = -1, last = -1):
        """
        Get a list of deletion runs in range first <= run <= last. If first == -1, pick only the latest before last.
        If last == -1, select runs up to the latest.
        """

        self.acquire_lock()
        try:
            run_numbers = self._do_get_deletion_runs(partition, first, last)
        finally:
            self.release_lock()

        return run_numbers

    def get_copy_runs(self, partition, first = -1, last = -1):
        """
        Get a list of copy runs in range first <= run <= last. If first == -1, pick only the latest before last.
        If last == -1, select runs up to the latest.
        """

        self.acquire_lock()
        try:
            run_numbers = self._do_get_copy_runs(partition, before)
        finally:
            self.release_lock()

        return run_number

    def get_run_timestamp(self, run_number):
        self.acquire_lock()
        try:
            timestamp = self._do_get_run_timestamp(run_number)
        finally:
            self.release_lock()

        return timestamp        

    def get_next_test_id(self):
        self.acquire_lock()
        try:
            test_id = self._do_get_next_test_id()
        finally:
            self.release_lock()

        return test_id

    def save_dataset_transfers(self,replica_list,replica_times):
        self.acquire_lock()
        try:
            self._do_save_dataset_transfers(replica_list,replica_times)
        finally:
            self.release_lock()

    def save_dataset_deletions(self,replica_list,replica_times):
        self.acquire_lock()
        try:
            self._do_save_replica_deletions(replica_list,replica_times)
        finally:
            self.release_lock()


if __name__ == '__main__':

    import sys
    import time
    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Local inventory store interface')

    parser.add_argument('command', metavar = 'COMMAND', help = '(update|check {copy|deletion} <operation_id>|snapshot [tag]|clean [tag]|restore <tag>|lock|release)')
    parser.add_argument('arguments', metavar = 'COMMAND', nargs = '*', help = '')
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

    if args.command == 'update':
        incomplete_copies = interface.get_incomplete_copies()
        
        for record in incomplete_copies:
            updates = copy_interface.copy_status(record.operation_id)

            # updates: {(site_name, dataset): (total_bytes, copied_bytes, time_update)}
            
            total = 0
            copied = 0
            for t, c, u in updates.values():
                total += t
                copied += c

            if copied == total:
                logger.info('Updating record for copy %d to %s.', record.operation_id, record.site_name)
        
                record.completed = True
                record.size = total
                interface.update_copy_entry(record)

    elif args.command == 'check':
        operation = args.arguments[0]

        try:
            operation_ids = map(int, args.arguments[1:])
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

    elif args.command == 'snapshot':
        try:
            tag = args.arguments[0]
        except IndexError:
            tag = ''

        interface.make_snapshot(tag = tag)

    elif args.command == 'list':
        try:
            what = args.arguments[0]
        except IndexError:
            print 'Usage: list (snapshots|datasets|sites)'

        if what == 'snapshots':
            for snapshot in interface.list_snapshots():
                print snapshot

    elif args.command == 'clean':
        try:
            tag = args.arguments[0]
        except IndexError:
            tag = ''

        interface.remove_snapshot(tag = tag)

    elif args.command == 'restore':
        tag = args.arguments[0]

        interface.recover_from(tag)

    elif args.command == 'lock':
        if len(args.arguments) > 0 and args.arguments[0] == 'block':
            interface.acquire_lock(blocking = True)
        else:
            interface.acquire_lock(blocking = False)

    elif args.command == 'release':
        interface.release_lock(force = True)
