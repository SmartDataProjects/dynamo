import os
import time
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

class ASCIIHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using ASCII files as backend.
    """

    def __init__(self, copy_requests = config.paths_base + '/data/copy_requests', copy_datasets = config.paths_base + '/data/copy_datasets', deletion_requests = config.paths_base + '/data/deletion_requests', deletion_datasets = config.paths_base + '/data/deletion_datasets', lock = config.paths_base + '/data/asciihistory_lock'):
        
        self.copy_requests_path = copy_requests
        self.copy_datasets_path = copy_datasets
        self.deletion_requests_path = deletion_requests
        self.deletion_datasets_path = deletion_datasets
        self.lock_path = lock

    def _do_acquire_lock(self, blocking): #override
        while os.path.exists(self.lock_path):
            if blocking:
                logger.warning('Failed to lock. Waiting 30 seconds..')
                time.sleep(30)
            else:
                logger.warning('Failed to lock.')
                return False

        open(self.lock_path, 'w').close()

        return True

    def _do_release_lock(self, force): #override
        os.remove(self.lock_path)

    def _do_make_copy_entry(self, site, operation_id, approved, do_list, size): #override
        with open(self.copy_requests_path, 'a') as requests:
            requests.write('%d %s %d 0 %d\n' % (operation_id, site.name, size, int(time.time())))

        with open(self.copy_datasets_path, 'a') as datasets:
            for dataset, origin in do_list:
                datasets.write('%d %s %s\n' % (operation_id, origin.name, dataset.name))

    def _do_make_deletion_entry(self, site, operation_id, approved, datasets, size): #override
        with open(self.deletion_requests_path, 'a') as requests:
            requests.write('%d %s %d %d\n' % (operation_id, site.name, size int(time.time())))

        with open(self.deletion_datasets_path, 'a') as datasets:
            for dataset in datasets:
                datasets.write('%d %s\n' % (operation_id, dataset.name))

    def _do_update_copy_entry(self, copy_record): #override
        with open(self.copy_requests_path, 'a') as requests:
            requests.write('%d %s %d %d %d\n' % (copy_record.operation_id, copy_record.site_name, copy_record.size, copy_record.done, copy_record.last_update))

    def _do_update_deletion_entry(self, deletion_record): #override
        with open(self.deletion_requests_path, 'a') as requests:
            requests.write('%d %s %d %d\n' % (deletion_record.operation_id, deletion_record.site_name, deletion_record.size, deletion_record.last_update))

    def _do_get_incomplete_copies(self): #override
        timestamps = {}
        records = []

        with open(self.copy_requests_path) as requests:
            lines = reversed(requests.readlines())

        for line in lines:
            words = line.split()
            operation_id = int(words[0])
            update = int(words[4])
            
            if operation_id in timestamps:
                timestamps[operation_id] = update
                continue

            size = int(words[2])
            done = int(words[3])

            # this is the first encounter to the operation id and size > done
            # -> is an incomplete operation
            if size > done:
                site_name = words[1]
                record = HistoryRecord(HistoryRecord.OP_COPY, operation_id, site_name, approved = True, size = size, done = done, last_update = update)
                records.append(record)

            timestamps[operation_id] = update

        for record in records:
            record.timestamp = timestamps[record.operation_id]

    def _do_get_site_name(self, operation_id): #override
        with open(self.copy_requests_path) as requests:
            for line in requests:
                if line.startswith('%d ' % operation_id):
                    return line.split()[1]

        with open(self.deletion_requests_path) as requests:
            for line in requests:
                if line.startswith('%d ' % operation_id):
                    return line.split()[1]

        return ''
