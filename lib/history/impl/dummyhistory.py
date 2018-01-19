import logging
import time

from dynamo.history.history import TransactionHistoryInterface
from dynamo.dataformat import Configuration, HistoryRecord

LOG = logging.getLogger(__name__)

class DummyHistory(TransactionHistoryInterface):
    """
    A history interface that does nothing.
    """

    def __init__(self, config):
        TransactionHistoryInterface.__init__(self, config)

    def _do_acquire_lock(self, blocking): #override
        return True

    def _do_release_lock(self, force): #override
        pass

    def _do_new_run(self, operation, partition, policy_version, comment): #override
        return 1

    def _do_close_run(self, operation, run_number): #override
        LOG.info('Cycle %d closed.', run_number)

    def _do_make_copy_entry(self, run_number, site, operation_id, approved, dataset_list, size): #override
        LOG.info('New copy entry: operation_id=%d approved=%d site=%s size=%d', operation_id, approved, site.name, size)

    def _do_make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size): #override
        LOG.info('New deletion entry: operation_id=%d approved=%d site=%s size=%d', operation_id, approved, site.name, size)

    def _do_update_copy_entry(self, copy_record): #override
        LOG.info('Update copy entry: operation_id=%d approved=%d size=%d completed=%d',
            copy_record.operation_id, copy_record.approved, copy_record.size, copy_record.completed)

    def _do_update_deletion_entry(self, deletion_record): #override
        LOG.info('Update deletion entry: operation_id=%d approved=%d size=%d completed=%d',
            copy_record.operation_id, copy_record.approved, copy_record.size, copy_record.completed)

    def _do_save_sites(self, sites): #override
        LOG.info('Saving %d sites', len(sites))

    def _do_get_sites(self, run_number): #override
        return {}

    def _do_save_datasets(self, datasets): #override
        LOG.info('Saving %d datasets', len(datasets))

    def _do_save_conditions(self, policy_lines): #ovrride
        LOG.info('Saving %d policy lines', len(policy_lines))

    def _do_save_copy_decisions(self, run_number, copies): #override
        LOG.info('Saving %d copy decisions', len(copies))

    def _do_save_deletion_decisions(self, run_number, deleted_list, kept_list, protected_list): #override
        LOG.info('Saving deletion decisions: %d delete, %d keep, %d protect', len(deleted_list), len(kept_list), len(protected_list))

    def _do_save_quotas(self, run_number, quotas): #override
        LOG.info('Saving quotas for %d sites', len(quotas))

    def _do_get_deletion_decisions(self, run_number, size_only): #override
        return {}

    def _do_save_dataset_popularity(self, run_number, datasets): #override
        LOG.info('Saving popularity for %d datasets', len(datasets))

    def _do_get_incomplete_copies(self, partition): #override
        return []

    def _do_get_copied_replicas(self, run_number): #override
        return []

    def _do_get_site_name(self, operation_id): #override
        return ''

    def _do_get_deletion_runs(self, partition, first, last): #override
        return []

    def _do_get_copy_runs(self, partition, first, last): #override
        return []

    def _do_get_run_timestamp(self, run_number): #override
        return 0

    def _do_get_next_test_id(self): #override
        return -1
