import logging
import time

from dynamo.history.history import TransactionHistoryInterface
from dynamo.dataformat import Configuration, HistoryRecord
from dynamo.utils.interface.mysql import MySQL

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

    def _do_new_cycle(self, operation, partition, policy_version, comment): #override
        try:
            return self.config['new_cycle']
        except KeyError:
            return 1

    def _do_close_cycle(self, operation, cycle_number): #override
        LOG.info('Cycle %d closed.', cycle_number)

    def _do_make_copy_entry(self, cycle_number, site, operation_id, approved, dataset_list, size): #override
        LOG.info('New copy entry: operation_id=%d approved=%d site=%s size=%d', operation_id, approved, site.name, size)

    def _do_make_deletion_entry(self, cycle_number, site, operation_id, approved, datasets, size): #override
        LOG.info('New deletion entry: operation_id=%d approved=%d site=%s size=%d', operation_id, approved, site.name, size)

    def _do_update_copy_entry(self, copy_record): #override
        LOG.info('Update copy entry: operation_id=%d approved=%d size=%d completed=%d',
            copy_record.operation_id, copy_record.approved, copy_record.size, copy_record.status)

    def _do_update_deletion_entry(self, deletion_record): #override
        LOG.info('Update deletion entry: operation_id=%d approved=%d size=%d completed=%d',
            copy_record.operation_id, copy_record.approved, copy_record.size, copy_record.status)

    def _do_save_sites(self, sites): #override
        LOG.info('Saving %d sites', len(sites))

    def _do_get_sites(self, cycle_number): #override
        try:
            return self.config['sites']
        except KeyError:
            return {}

    def _do_save_datasets(self, datasets): #override
        LOG.info('Saving %d datasets', len(datasets))

    def _do_save_conditions(self, policy_lines): #ovrride
        LOG.info('Saving %d policy lines', len(policy_lines))

    def _do_save_copy_decisions(self, cycle_number, copies): #override
        LOG.info('Saving %d copy decisions', len(copies))

    def _do_save_deletion_decisions(self, cycle_number, deleted_list, kept_list, protected_list): #override
        LOG.info('Saving deletion decisions: %d delete, %d keep, %d protect', len(deleted_list), len(kept_list), len(protected_list))

    def _do_save_quotas(self, cycle_number, quotas): #override
        LOG.info('Saving quotas for %d sites', len(quotas))

    def _do_get_deletion_decisions(self, cycle_number, size_only): #override
        if 'deletion_decisions' in self.config:
            return self.config['deletion_decisions']

        elif 'db_params' in self.config and 'cache_db_params' in self.config:
            db_name = self.config.db_params.db
            cache_db = MySQL(self.config.cache_db_params)

            table_name = 'replicas_%d' % cycle_number
    
            query = 'SELECT s.`name`, d.`name`, r.`size`, r.`decision`, p.`text` FROM `%s`.`%s` AS r' % (cache_db.db_name(), table_name)
            query += ' INNER JOIN `%s`.`sites` AS s ON s.`id` = r.`site_id`' % db_name
            query += ' INNER JOIN `%s`.`datasets` AS d ON d.`id` = r.`dataset_id`' % db_name
            query += ' INNER JOIN `%s`.`policy_conditions` AS p ON p.`id` = r.`condition`' % db_name
            query += ' ORDER BY s.`name` ASC, r.`size` DESC'
    
            product = {}
    
            _site_name = ''
            
            for site_name, dataset_name, size, decision, reason in cache_db.xquery(query):
                if site_name != _site_name:
                    product[site_name] = []
                    current = product[site_name]
                    _site_name = site_name
                
                current.append((dataset_name, size, decision, reason))
    
            return product

        else:
            return {}

    def _do_save_dataset_popularity(self, cycle_number, datasets): #override
        LOG.info('Saving popularity for %d datasets', len(datasets))

    def _do_get_incomplete_copies(self, partition): #override
        try:
            return self.config['incomplete_copies']
        except KeyError:
            return []            

    def _do_get_copied_replicas(self, cycle_number): #override
        try:
            return self.config['copied_replicas'][cycle_number]
        except KeyError:
            return []

    def _do_get_site_name(self, operation_id): #override
        try:
            return self.config['site_name'][operation_id]
        except KeyError:
            return ''

    def _do_get_deletion_cycles(self, partition, first, last): #override
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

    def _do_get_copy_cycles(self, partition, first, last): #override
        try:
            cycles = self.config['copy_cycles'][partition]
        except KeyError:
            return []
        else:
            if first != -1:
                cycles = filter(lambda r: r >= first, cycles)
            if last != -1:
                cycles = filter(lambda r: r <= last, cycles)

    def _do_get_cycle_timestamp(self, cycle_number): #override
        try:
            return self.config['cycle_timestamp'][cycle_number]
        except KeyError:
            return 0

    def _do_get_next_test_id(self): #override
        return -1
