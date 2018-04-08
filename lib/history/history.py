import logging

from dynamo.dataformat import Configuration, HistoryRecord

LOG = logging.getLogger(__name__)

class TransactionHistoryInterface(object):
    """
    Interface for transaction history.
    """

    def __init__(self, config):
        self.test = config.get('test', False)

    def new_copy_cycle(self, partition, policy_version, comment = ''):
        """
        Set up a new copy cycle for the partition.
        @param partition        partition name string
        @param policy_version   string for policy version
        @param comment          comment string

        @return cycle number.
        """
        raise NotImplementedError('new_copy_cycle')

    def new_deletion_cycle(self, partition, policy_version, comment = ''):
        """
        Set up a new deletion cycle for the partition.
        @param partition        partition name string
        @param policy_version   string for policy version
        @param comment          comment string

        @return cycle number.
        """
        raise NotImplementedError('new_deletion_cycle')

    def close_copy_cycle(self, cycle_number):
        """
        Finalize the records for the given cycle.
        @param cycle_number   Cycle number
        """
        raise NotImplementedError('close_copy_cycle')

    def close_deletion_cycle(self, cycle_number):
        """
        Finalize the records for the given cycle.
        @param cycle_number   Cycle number
        """
        raise NotImplementedError('close_deletion_cycle')

    def make_copy_entry(self, cycle_number, site, operation_id, approved, datasets, size):
        """
        Record a copy operation.
        @param cycle_number  Cycle number
        @param site          Site object of the copy destination
        @param operation_id  Operation id returned by the copy operation
        @param approved      Boolean
        @param datasets      List of Dataset objects
        @param size          Total size to be copied
        """
        raise NotImplementedError('make_copy_entry')

    def make_deletion_entry(self, cycle_number, site, operation_id, approved, datasets, size):
        """
        Record a deletion operation.
        @param cycle_number  Cycle number
        @param site          Site object of the deletion operation
        @param operation_id  Operation id returned by the deletion operation
        @param approved      Boolean
        @param datasets      List of Dataset objects
        @param size          Total size to be deleted
        """
        raise NotImplementedError('make_deletion_entry')

    def update_copy_entry(self, copy_record):
        """
        Update a copy entry. Only certain fields (approved, last_update) are updatable.
        @param copy_record   HistoryRecord object.
        """
        raise NotImplementedError('update_copy_entry')

    def update_deletion_entry(self, deletion_record):
        """
        Update a deletion entry. Only certain fields (approved, last_update) are updatable.
        @param deletion_record   HistoryRecord object.
        """
        raise NotImplementedError('update_deletion_entry')

    def save_sites(self, sites):
        """
        Save site names.
        @param sites       List of Site objects
        """
        raise NotImplementedError('save_sites')

    def save_datasets(self, datasets):
        """
        Save dataset names.
        @param datasets    List of Dataset objects
        """
        raise NotImplementedError('save_datasets')

    def get_incomplete_copies(self, partition):
        """
        Get a list of incomplete copies.
        @param partition   partition name

        @return list of HistoryRecords
        """
        raise NotImplementedError('get_incomplete_copies')

    def get_site_name(self, operation_id):
        """
        Get the copy or deletion target site name for the given operation.
        @param operation_id   Copy or deletion operation id

        @return site name string (empty if not operation is found)
        """
        raise NotImplementedError('get_site_name')

    def get_deletion_cycles(self, partition, first = -1, last = -1):
        """
        Get a list of deletion cycles in range first <= cycle <= last. If first == -1, pick only the latest before last.
        If last == -1, select cycles up to the latest.
        @param partition  partition name
        @param first      first cycle
        @param last       last cycle

        @return list of cycle numbers
        """
        raise NotImplementedError('get_deletion_cycles')

    def get_copy_cycles(self, partition, first = -1, last = -1):
        """
        Get a list of copy cycles in range first <= cycle <= last. If first == -1, pick only the latest before last.
        If last == -1, select cycles up to the latest.
        @param partition  partition name
        @param first      first cycle
        @param last       last cycle

        @return list of cycle numbers
        """
        raise NotImplementedError('get_copy_cycles')

    def get_cycle_timestamp(self, cycle_number):
        """
        Get the timestamp of the copy or deletion cycle.
        @param cycle_number  Cycle number

        @return UNIX timestamp of the cycle.
        """
        raise NotImplementedError('get_cycle_timestamp')
