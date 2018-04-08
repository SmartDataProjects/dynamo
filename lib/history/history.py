import logging

from dynamo.dataformat import Configuration, HistoryRecord

LOG = logging.getLogger(__name__)

class TransactionHistoryInterface(object):
    """
    Interface for transaction history.
    """

    def __init__(self, config):
        self.config = Configuration(config)

    def new_copy_cycle(self, partition, policy_version, comment = ''):
        """
        Set up a new copy/deletion cycle for the partition.
        """

        self.acquire_lock()
        try:
            cycle_number = self._do_new_cycle(HistoryRecord.OP_COPY, partition, policy_version, comment)
        finally:
            self.release_lock()

        return cycle_number

    def new_deletion_cycle(self, partition, policy_version, comment = ''):
        """
        Set up a new copy/deletion cycle for the partition.
        """

        self.acquire_lock()
        try:
            cycle_number = self._do_new_cycle(HistoryRecord.OP_DELETE, partition, policy_version, comment)
        finally:
            self.release_lock()

        return cycle_number

    def close_copy_cycle(self, cycle_number):
        self.acquire_lock()
        try:
            self._do_close_cycle(HistoryRecord.OP_COPY, cycle_number)
        finally:
            self.release_lock()

    def close_deletion_cycle(self, cycle_number):
        self.acquire_lock()
        try:
            self._do_close_cycle(HistoryRecord.OP_DELETE, cycle_number)
        finally:
            self.release_lock()

    def make_copy_entry(self, cycle_number, site, operation_id, approved, dataset_list, size):
        if self.config.get('test', False) or cycle_number == 0:
            # Don't do anything
            return

        self.acquire_lock()
        try:
            self._do_make_copy_entry(cycle_number, site, operation_id, approved, dataset_list, size)
        finally:
            self.release_lock()

    def make_deletion_entry(self, cycle_number, site, operation_id, approved, datasets, size):
        if self.config.get('test', False) or cycle_number == 0:
            # Don't do anything
            return

        self.acquire_lock()
        try:
            self._do_make_deletion_entry(cycle_number, site, operation_id, approved, datasets, size)
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

    def get_sites(self, cycle_number = 0, partition = ''):
        """
        Collect the site status for a given cycle number or the latest cycle of the partition
        and return as a plain dict.
        """

        if cycle_number == 0:
            deletion_cycles = self.get_deletion_cycles(partition)
            copy_cycles = self.get_copy_cycles(partition)
            if len(deletion_cycles) == 0 and len(copy_cycles) == 0:
                raise CycletimeError('No history record exists')
            elif len(deletion_cycles) == 0:
                cycle_number = copy_cycles[0]
            elif len(copy_cycles) == 0:
                cycle_number = deletion_cycles[0]
            else:
                cycle_number = max(deletion_cycles[0], copy_cycles[0])

        self.acquire_lock()
        try:
            sites_info = self._do_get_sites(cycle_number)
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

    def save_copy_decisions(self, cycle_number, copies):
        """
        Save reasons for copy decisions? Still deciding what to do..
        """

        self.acquire_lock()
        try:
            self._do_save_copy_decisions(cycle_number, copies)
        finally:
            self.release_lock()
      
    def save_deletion_decisions(self, cycle_number, deleted_list, kept_list, protected_list):
        """
        Save decisions and their reasons for all replicas.
        @param cycle_number      Cycle number.
        @param deleted_list    {replica: [([block_replica], condition)]}
        @param kept_list       {replica: [([block_replica], condition)]}
        @param protected_list  {replica: [([block_replica], condition)]}

        Note that in case of block-level operations, one dataset replica can appear
        in multiple of deleted, kept, and protected.
        """

        self.acquire_lock()
        try:
            self._do_save_deletion_decisions(cycle_number, deleted_list, kept_list, protected_list)
        finally:
            self.release_lock()

    def save_quotas(self, cycle_number, quotas):
        """
        Save the site partition quotas for the cycle.
        @param cycle_number     Cycle number.
        @param quotas         {site: quota in TB}
        """

        self.acquire_lock()
        try:
            self._do_save_quotas(cycle_number, quotas)
        finally:
            self.release_lock()

    def get_deletion_decisions(self, cycle_number, size_only = True):
        """
        Return a dict {site: (protect_size, delete_size, keep_size)} if size_only = True.
        Else return a massive dict {site: [(dataset, size, decision, reason)]}
        """

        self.acquire_lock()
        try:
            decisions = self._do_get_deletion_decisions(cycle_number, size_only)
        finally:
            self.release_lock()

        return decisions

    def save_dataset_popularity(self, cycle_number, datasets):
        """
        Second argument popularities is a list [(dataset, popularity_score)].
        """

        self.acquire_lock()
        try:
            self._do_save_dataset_popularity(cycle_number, datasets)
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

    def get_copied_replicas(self, cycle_number):
        """
        Get the list of (site name, dataset name) copied in the given cycle.
        """
        self.acquire_lock()
        try:
            # list of HistoryRecords
            copies = self._do_get_copied_replicas(cycle_number)
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

    def get_deletion_cycles(self, partition, first = -1, last = -1):
        """
        Get a list of deletion cycles in range first <= cycle <= last. If first == -1, pick only the latest before last.
        If last == -1, select cycles up to the latest.
        """

        self.acquire_lock()
        try:
            cycle_numbers = self._do_get_deletion_cycles(partition, first, last)
        finally:
            self.release_lock()

        return cycle_numbers

    def get_copy_cycles(self, partition, first = -1, last = -1):
        """
        Get a list of copy cycles in range first <= cycle <= last. If first == -1, pick only the latest before last.
        If last == -1, select cycles up to the latest.
        """

        self.acquire_lock()
        try:
            cycle_numbers = self._do_get_copy_cycles(partition, before)
        finally:
            self.release_lock()

        return cycle_number

    def get_cycle_timestamp(self, cycle_number):
        self.acquire_lock()
        try:
            timestamp = self._do_get_cycle_timestamp(cycle_number)
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
