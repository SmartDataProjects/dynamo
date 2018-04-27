import logging

from dynamo.history.history import TransactionHistoryInterface

LOG = logging.getLogger(__name__)

class DummyHistory(TransactionHistoryInterface):
    """
    A history interface that does nothing.
    """

    def __init__(self, config):
        TransactionHistoryInterface.__init__(self, config)
        self.config = config.clone()

        self.incomplete_copies = []
        if 'incomplete_copies' in config:
            # fill it!
            pass

    def new_copy_cycle(self, partition, policy_version, comment = ''): #override
        return self.config['new_cycle']

    def new_deletion_cycle(self, partition, policy_version, comment = ''): #override
        return self.config['new_cycle']

    def close_copy_cycle(self, cycle_number): #override
        LOG.info('Cycle %d closed.', cycle_number)

    def close_deletion_cycle(self, cycle_number): #override
        LOG.info('Cycle %d closed.', cycle_number)

    def make_copy_entry(self, cycle_number, site, operation_id, approved, dataset_list): #override
        LOG.info('New copy entry: operation_id=%d approved=%d site=%s', operation_id, approved, site.name)

    def make_deletion_entry(self, cycle_number, site, operation_id, approved, dataset_list): #override
        LOG.info('New deletion entry: operation_id=%d approved=%d site=%s', operation_id, approved, site.name)

    def update_copy_entry(self, copy_record): #override
        LOG.info('Update copy entry: operation_id=%d approved=%d replicas=%d',
            copy_record.operation_id, copy_record.approved, len(copy_record.replicas))

    def update_deletion_entry(self, deletion_record): #override
        LOG.info('Update deletion entry: operation_id=%d approved=%d replicas=%d',
            deletion_record.operation_id, deletion_record.approved, len(deletion_record.replicas))

    def save_sites(self, sites): #override
        LOG.info('Saving %d sites', len(sites))

    def save_datasets(self, datasets): #override
        LOG.info('Saving %d datasets', len(datasets))

    def get_incomplete_copies(self, partition): #override
        return self.incomplete_copies

    def get_site_name(self, operation_id): #override
        try:
            return self.config['site_name'][operation_id]
        except KeyError:
            return ''

    def get_deletion_cycles(self, partition, first = -1, last = -1): #override
        try:
            cycles = self.config['deletion_cycles'][partition]
        except KeyError:
            return []
        else:
            if first != -1:
                cycles = filter(lambda r: r >= first, cycles)
            if last != -1:
                cycles = filter(lambda r: r <= last, cycles)
                
            return cycles

    def get_copy_cycles(self, partition, first = -1, last = -1): #override
        try:
            cycles = self.config['copy_cycles'][partition]
        except KeyError:
            return []
        else:
            if first != -1:
                cycles = filter(lambda r: r >= first, cycles)
            if last != -1:
                cycles = filter(lambda r: r <= last, cycles)

    def get_cycle_timestamp(self, cycle_number): #override
        try:
            return self.config['cycle_timestamp'][cycle_number]
        except KeyError:
            return 0
