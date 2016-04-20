import logging

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

    def make_copy_entry(self, site, operation_id, approved, ro_list, size):
        if config.read_only:
            logger.info('make_copy_entry')
            return

        self.acquire_lock()
        try:
            self._do_make_copy_entry(site, operation_id, approved, ro_list, size)
        finally:
            self.release_lock()

    def make_deletion_entry(self, site, operation_id, approved, datasets, size):
        if config.read_only:
            logger.info('make_deletion_entry')
            return

        self.acquire_lock()
        try:
            self._do_make_deletion_entry(site, operation_id, approved, datasets, size)
        finally:
            self.release_lock()
