import logging

from dynamo.dataformat import Configuration, HistoryRecord

LOG = logging.getLogger(__name__)

class TransactionHistoryInterface(object):
    """
    Interface for transaction history. Has a locking mechanism similar to store.
    """

    def __init__(self, config):
        self._lock_depth = 0
        self.config = Configuration(config)

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

    def new_copy_run(self, partition, policy_version, comment = ''):
        """
        Set up a new copy/deletion run for the partition.
        """

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_COPY, partition, policy_version, comment)
        finally:
            self.release_lock()

        return run_number

    def new_deletion_run(self, partition, policy_version, comment = ''):
        """
        Set up a new copy/deletion run for the partition.
        """

        self.acquire_lock()
        try:
            run_number = self._do_new_run(HistoryRecord.OP_DELETE, partition, policy_version, comment)
        finally:
            self.release_lock()

        return run_number

    def close_copy_run(self, run_number):
        self.acquire_lock()
        try:
            self._do_close_run(HistoryRecord.OP_COPY, run_number)
        finally:
            self.release_lock()

    def close_deletion_run(self, run_number):
        self.acquire_lock()
        try:
            self._do_close_run(HistoryRecord.OP_DELETE, run_number)
        finally:
            self.release_lock()

    def make_copy_entry(self, run_number, site, operation_id, approved, dataset_list, size):
        if self.config.get('test', False):
            # Don't do anything
            return

        self.acquire_lock()
        try:
            self._do_make_copy_entry(run_number, site, operation_id, approved, dataset_list, size)
        finally:
            self.release_lock()

    def make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size):
        if self.config.get('test', False):
            # Don't do anything
            return

        self.acquire_lock()
        try:
            self._do_make_deletion_entry(run_number, site, operation_id, approved, datasets, size)
        finally:
            self.release_lock()

    def update_copy_entry(self, copy_record):
        """
        Update copy entry from the argument. Only certain fields (approved, last_update) are updatable.
        """

        self.acquire_lock()
        try:
            self._do_update_copy_entry(copy_record)
        finally:
            self.release_lock()

    def update_deletion_entry(self, deletion_record):
        """
        Update deletion entry from the argument. Only certain fields (approved, last_update) are updatable.
        """

        self.acquire_lock()
        try:
            self._do_update_deletion_entry(deletion_record)
        finally:
            self.release_lock()

    def save_sites(self, sites):
        """
        Save status of sites.
        @param sites       List of sites
        """

        self.acquire_lock()
        try:
            self._do_save_sites(sites)
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

    def save_datasets(self, datasets):
        """
        Save datasets that are in the inventory but not in the history records.
        """

        self.acquire_lock()
        try:
            self._do_save_datasets(datasets)
        finally:
            self.release_lock()

    def save_conditions(self, policy_lines):
        """
        Save policy conditions.
        """

        self.acquire_lock()
        try:
            self._do_save_conditions(policy_lines)
        finally:
            self.release_lock()

    def save_copy_decisions(self, run_number, copies):
        """
        Save reasons for copy decisions? Still deciding what to do..
        """

        self.acquire_lock()
        try:
            self._do_save_copy_decisions(run_number, copies)
        finally:
            self.release_lock()
      
    def save_deletion_decisions(self, run_number, deleted_list, kept_list, protected_list):
        """
        Save decisions and their reasons for all replicas.
        @param run_number      Cycle number.
        @param deleted_list    {replica: [([block_replica], condition)]}
        @param kept_list       {replica: [([block_replica], condition)]}
        @param protected_list  {replica: [([block_replica], condition)]}

        Note that in case of block-level operations, one dataset replica can appear
        in multiple of deleted, kept, and protected.
        """

        self.acquire_lock()
        try:
            self._do_save_deletion_decisions(run_number, deleted_list, kept_list, protected_list)
        finally:
            self.release_lock()

    def save_quotas(self, run_number, quotas):
        """
        Save the site partition quotas for the cycle.
        @param run_number     Cycle number.
        @param quotas         {site: quota in TB}
        """

        self.acquire_lock()
        try:
            self._do_save_quotas(run_number, quotas)
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
