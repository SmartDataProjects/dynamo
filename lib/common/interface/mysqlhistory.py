import os
import socket
import logging
import time
import collections
import array

from common.interface.history import TransactionHistoryInterface
from common.interface.mysql import MySQL
from common.dataformat import HistoryRecord
import common.configuration as config

logger = logging.getLogger(__name__)

class MySQLHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self):
        super(self.__class__, self).__init__()

        self._mysql = MySQL(**config.mysqlhistory.db_params)

        self._site_id_map = {}
        self._dataset_id_map = {}

    def _do_acquire_lock(self, blocking): #override
        while True:
            # Use the system table to "software-lock" the database
            self._mysql.query('LOCK TABLES `lock` WRITE')
            self._mysql.query('UPDATE `lock` SET `lock_host` = %s, `lock_process` = %s WHERE `lock_host` LIKE \'\' AND `lock_process` = 0', socket.gethostname(), os.getpid())

            # Did the update go through?
            host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
            self._mysql.query('UNLOCK TABLES')

            if host == socket.gethostname() and pid == os.getpid():
                # The database is locked.
                break
            
            if blocking:
                logger.warning('Failed to lock database. Waiting 30 seconds..')
                time.sleep(30)
            else:
                logger.warning('Failed to lock database.')
                return False

        return True

    def _do_release_lock(self, force): #override
        self._mysql.query('LOCK TABLES `lock` WRITE')
        if force:
            self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0')
        else:
            self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise TransactionHistoryInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_make_snapshot(self, tag): #override
        new_db = self._mysql.make_snapshot(tag)

        self._mysql.query('UPDATE `%s`.`lock` SET `lock_host` = \'\', `lock_process` = 0' % new_db)

    def _do_remove_snapshot(self, tag, newer_than, older_than): #override
        self._mysql.remove_snapshot(tag = tag, newer_than = newer_than, older_than = older_than)

    def _do_list_snapshots(self, timestamp_only): #override
        return self._mysql.list_snapshots(timestamp_only)

    def _do_recover_from(self, tag): #override
        self._mysql.recover_from(tag)

    def _do_new_run(self, operation, partition, policy_version, is_test, comment): #override
        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            part_id = self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
        else:
            part_id = part_ids[0]

        if operation == HistoryRecord.OP_COPY:
            if is_test:
                operation_str = 'copy_test'
            else:
                operation_str = 'copy'
        else:
            if is_test:
                operation_str = 'deletion_test'
            else:
                operation_str = 'deletion'

        return self._mysql.query('INSERT INTO `runs` (`operation`, `partition_id`, `policy_version`, `comment`, `time_start`) VALUES (%s, %s, %s, FROM_UNIXTIME(%s))', operation_str, part_id, policy_version, comment, time.time())

    def _do_close_run(self, operation, run_number): #override
        self._mysql.query('UPDATE `runs` SET `time_end` = FROM_UNIXTIME(%s) WHERE `id` = %s', time.time(), run_number)

    def _do_make_copy_entry(self, run_number, site, operation_id, approved, dataset_list, size): #override
        """
        Site and datasets are expected to be already in the database.
        """

        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        self._mysql.query('INSERT INTO `copy_requests` (`id`, `run_id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', operation_id, run_number, approved, self._site_id_map[site.name], size)

        self._mysql.insert_many('copied_replicas', ('copy_id', 'dataset_id'), lambda d: (operation_id, self._dataset_id_map[d.name]), dataset_list)

    def _do_make_deletion_entry(self, run_number, site, operation_id, approved, datasets, size): #override
        """
        site and dataset are expected to be already in the database (save_deletion_decisions should be called first).
        """

        site_id = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)[0]

        dataset_ids = self._mysql.select_many('datasets', ('id',), 'name', ['\'%s\'' % d.name for d in datasets])

        self._mysql.query('INSERT INTO `deletion_requests` (`id`, `run_id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', operation_id, run_number, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda did: (operation_id, did), dataset_ids)

    def _do_update_copy_entry(self, copy_record): #override
        self._mysql.query('UPDATE `copy_requests` SET `approved` = %s, `size_copied` = %s, `last_update` = FROM_UNIXTIME(%s) WHERE `id` = %s', copy_record.approved, copy_record.done, copy_record.last_update, copy_record.operation_id)
        
    def _do_update_deletion_entry(self, deletion_record): #override
        self._mysql.query('UPDATE `deletion_requests` SET `approved` = %s, `size_deleted` = %s, `last_update` = FROM_UNIXTIME(%s) WHERE `id` = %s', deletion_record.approved, deletion_record.done, deletion_record.last_update, deletion_record.operation_id)

    def _do_save_sites(self, run_number, inventory): #override
        for site in inventory.sites.values():
            # site statuses are packed in 32-bit words with run(24)-active(4)-status(4)
            st_arr = array.array('I', [])

            record = self._mysql.query('SELECT `id`, `status` FROM `sites` WHERE `name` LIKE %s', site.name)

            if len(record) == 0:
                st_arr.append((run_number << 8) + (site.active << 4) + site.status)
                self._mysql.query('INSERT INTO `sites` (`name`, `status`) SET (%s, %s)', site.name, st_arr.tostring())

            else:
                site_id, status_blob = record[0]

                st_arr.fromstring(status_blob)

                active = (st_arr[-1] >> 4) & 0xf
                status = st_arr[-1] & 0xf

                if active != site.active or status != site.status:
                    st_arr.insert(irun, (run_number << 8) + (site.active << 4) + site.status)
                    self._mysql.query('UPDATE `sites` SET `status` = %s WHERE `id` = %s', st_arr.tostring(), site_id)

    def _do_get_sites(self, run_number): #override
        partition_id = self._mysql.query('SELECT `partition_id` FROM runs WHERE `id` = %s', run_number)[0]
        quota_blob_map = dict(self._mysql.query('SELECT `site_id`, `quotas` FROM `partition_quotas` WHERE `partition_id` = %s', partition_id))

        sites_dict = {}

        # site quotas are packed in 64-bit words with run(32)-quota(32)
        # site statuses are packed in 32-bit words with run(24)-active(4)-status(4)
        q_arr = array.array('L', [])
        st_arr = array.array('I', [])

        for site_id, site_name, status_blob in self._mysql.query('SELECT `id`, `name`, `status` FROM `sites`'):
            try:
                q_arr.fromstring(quota_blob_map[site_id])
                quota = q_arr[-1] & 0xffffffff

            except KeyError:
                quota = 0
                
            st_arr.fromstring(status_blob)
            active = (st_arr[-1] >> 4) & 0xf
            status = st_arr[-1] & 0xf

            sites_dict[site_name] = (active, status, quota)

        return sites_dict

    def _do_save_datasets(self, run_number, inventory): #override
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        datasets_to_insert = []
        for dataset_name in inventory.datasets.keys():
            if dataset_name not in self._dataset_id_map:
                datasets_to_insert.append(dataset_name)

        if len(datasets_to_insert) == 0:
            return

        self._mysql.insert_many('datasets', ('name',), lambda n: (n,), datasets_to_insert)
        self._make_dataset_id_map()

    def _do_save_quotas(self, run_number, quotas, inventory): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        partition_id = self._mysql.query('SELECT `partition_id` FROM runs WHERE `id` = %s', run_number)[0]

        select_query = 'SELECT `quotas` FROM `partition_quotas` WHERE `site_id` = %s AND `partition_id` = {partition_id}'.format(partition_id = partition_id)

        for site, quota in quotas.items():
            # site is expected to exist in history record at this point
            site_id = self._site_id_map[site.name]
                
            q_arr = array.array('L', [])
            record = self._mysql.query(select_query, site_id)

            if len(record) == 0:
                q_arr.append((run_number << 32) + quota)
                self._mysql.query('INSERT INTO `partition_quotas` (`site_id`, `partition_id`, `quotas`) VALUES (%s, %s, %s)', site_id, partition_id, q_arr.tostring())

            else:
                q = q_arr[-1] & 0xffffffff

                if q != quota:
                    q_arr.insert(irun, (run_number << 32) + quota)
                    self._mysql.query('UPDATE `partition_quotas` SET `quotas` = %s WHERE `site_id` = %s AND `partition_id` = %s', st_arr.tostring(), site_id, partition_id)

    def _do_save_conditions(self, policies):
        for policy in policies:
            text = re.sub('\s+', ' ', policy.condition.text)
            ids = self._mysql.query('SELECT `id` FROM `policy_conditions` WHERE `text` LIKE %s', text)
            if len(ids) == 0:
                policy.condition_id = self._mysql.query('INSERT INTO `policy_conditions` (`text`) VALUES (%s)', text)
            else:
                policy.condition_id = ids[0]

    def _do_save_replicas(self, run_number, replicas): #override
        # (site_id, dataset_id) -> replica in inventory
        indices_to_replicas = self._make_replica_map(replicas)

        operation, partition_id = self._mysql.query('SELECT `operation`, `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        sz_arr = array.array('L', [])

        # updating replicas in record with deleted = 0
        for site_id, dataset_id, sizes_blob in self._mysql.query('SELECT `site_id`, `dataset_id`, `sizes` FROM `replicas` WHERE `partition_id` = %s AND `deleted` = 0', partition_id):
            index = (site_id, dataset_id)
            sz_arr.fromstring(sizes_blob)

            try:
                replica = indices_to_replicas.pop(index)
            except KeyError:
                sz_arr.append((run_number << 32))
                self._mysql.query('UPDATE `replicas` SET `deleted` = 1, `sizes` = %s WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s', sz_arr.tostring(), site_id, dataset_id, partition_id)

                continue

            current_size = int(round(replica.size() * 1.e-6))

            if (sz_arr[-1] & 0xffffffff) == current_size:
                continue
                
            sz_arr.append((run_number << 32) + current_size)

            self._mysql.query('UPDATE `replicas` SET `deleted` = 0, `sizes` = %s WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s', sz_arr.tostring(), site_id, dataset_id, partition_id)

        # updating replicas that were deleted but resurrected
        for site_id, dataset_id, sizes_blob in self._mysql.select_many('replicas', ('site_id', 'dataset_id', 'sizes'), ('site_id', 'dataset_id'), indices_to_replicas.keys(), additional_conditions = ['`partition_id` = %d' % partition_id, '`deleted` = 1']):
            replica = indices_to_replicas.pop(index)

            current_size = int(round(replica.size() * 1.e-6))

            if (sz_arr[-1] & 0xffffffff) == current_size:
                continue
                
            sz_arr.append((run_number << 32) + current_size)

            self._mysql.query('UPDATE `replicas` SET `deleted` = 0, `sizes` = %s WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s', sz_arr.tostring(), site_id, dataset_id, partition_id)

        # inserting new replicas
        sz_arr = array.array('L', [])
        def size_to_string(size):
            sz_arr[0] = (run_number << 32) + size
            return sz_arr.tostring()

        fields = ('site_id', 'dataset_id', 'partition_id', 'deleted', 'sizes')
        mapping = lambda (index, replica): (index[0], index[1], partition_id, 0, size_to_string(int(round(replica.size() * 1.e-6))))
        self._mysql.insert_many('replicas', fields, mapping, indices_to_replicas.items())

    def _do_save_copy_decisions(self, run_number, copies): #override
        pass

    def _do_save_deletion_decisions(self, run_number, decisions, delete_val): #override
        partition_id = self._mysql.query('SELECT `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        indices_to_replicas = self._make_replica_map(protected.keys() + deleted.keys() + kept.keys())

        dec_arr = array.array('L', [])

        for site_id, dataset_id, decisions_blob in self._mysql.query('SELECT `site_id`, `dataset_id`, `deletion_decisions` FROM `replicas` WHERE `partition_id` = %s AND `deleted` = 0', partition_id):
            replica = indices_to_replicas[(site_id, dataset_id)]
            try:
                decision, condition_id = decisions[replica]
            except KeyError:
                # should not happen, but we can just set deleted to 1
                sizes_blob = self._mysql.query('SELECT `sizes` FROM `replicas` WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s', site_id, dataset_id, partition_id)[0]
                sz_arr = array.array('L', [])
                sz_arr.fromstring(sizes_blob)
                sz_arr.append(run_number << 32)
                
                self._mysql.query('UPDATE `replicas` SET `deleted` = 1, `sizes` = %s WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s', sz_arr.tostring(), site_id, dataset_id, partition_id)
                continue

            dec_arr.fromstring(decisions_blob)

            if len(dec_arr) == 0 or ((dec_arr[-1] >> 32) & 0x3) != decision or (dec_arr[-1] & 0xffffffff) != condition_id:
                dec_arr.append((run_number << 34) + (decision << 32) + condition_id)
                query = 'UPDATE `replicas` SET `deletion_decisions` = %s'
                if decision == delete_val:
                    query += ', `deleted` = 1'
                query += ' WHERE `site_id` = %s AND `dataset_id` = %s AND `partition_id` = %s'

                self._mysql.query(query, dec_arr.tostring(), site_id, dataset_id, partition_id)

    def _do_get_deletion_decisions(self, run_number, size_only): #override
        partition_id = self._mysql.query('SELECT `operation`, `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        sz_arr = array.array('L', [])
        dec_arr = array.array('L', [])
        if size_only:
            tmp_dict = {}
            
            query = 'SELECT s.`name`, r.`sizes`, r.`deletion_decisions` FROM `replicas` AS r'
            query += ' INNER JOIN `sites` AS s ON s.`id` = r.`site_id`'
            query += ' WHERE r.`partition_id` = %s'
            for site_name, sizes_blob, decisions_blob in self._mysql.query(query, partition_id):
                sz_arr.fromstring(sizes_blob)
                dec_arr.fromstring(decisions_blob)

                for word in sz_arr:
                    if (word >> 32) > run_number:
                        break
                    size = (word & 0xffffffff)

                for word in dec_arr:
                    if (word >> 34) > run_number:
                        break
                    decision = (word >> 32) & 0x3

                if site_name not in tmp_dict:
                    # cannot be fully agnostic about decision values here
                    tmp_dict[site_name] = {1: 0, 2: 0, 3: 0}

                tmp_dict[site_name][decision] += size

            product = {}
            for site_name, tmp_cont in tmp_dict.items():
                product[site_name] = (tmp_cont[1], tmp_cont[2], tmp_cont[3])

            return product

        else:
            # implement later
            return {}

    def _do_save_dataset_popularity(self, run_number, datasets): #override
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        fields = ('run_id', 'dataset_id', 'popularity')
        mapping = lambda dataset: (run_number, self._dataset_id_map[dataset.name], dataset.demand.request_weight)
        self._mysql.insert_many('dataset_popularity_snapshots', fields, mapping, datasets)

    def _do_get_incomplete_copies(self, partition): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`, h.`size_copied`, UNIX_TIMESTAMP(h.`last_update`) FROM `copy_requests` AS h INNER JOIN `runs` AS r ON r.`id` = h.`run_id` INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id` INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`id` > 0 AND p.`name` LIKE %s AND h.`size` != h.`size_copied`', partition)
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size, size_copied, last_update in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_COPY, eid, site_name, timestamp = timestamp, approved = approved, size = size, done = size_copied, last_update = last_update)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('copied_replicas', ('copy_id', 'dataset_id'), 'copy_id', ['%d' % i for i in id_to_record.keys()])

        current_copy_id = 0
        for copy_id, dataset_id in replicas:
            if copy_id != current_copy_id:
                record = id_to_record[copy_id]
                current_copy_id = copy_id

            record.replicas.append(HistoryRecord.CopiedReplica(dataset_name = id_to_dataset[dataset_id]))

        return id_to_record.values()

    def _do_get_site_name(self, operation_id): #override
        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `copy_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        result = self._mysql.query('SELECT s.name FROM `sites` AS s INNER JOIN `deletion_requests` AS h ON h.`site_id` = s.`id` WHERE h.`id` = %s', operation_id)
        if len(result) != 0:
            return result[0]

        return ''

    def _do_get_latest_deletion_run(self, partition): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return 0

        partition_id = result[0]
        result = self._mysql.query('SELECT `id` FROM `runs` WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'deletion\', \'deletion_test\') ORDER BY `id` DESC LIMIT 1', partition_id)
        if len(result) == 0:
            return 0

        return result[0]

    def _do_get_run_timestamp(self, run_number): #override
        result = self._mysql.query('SELECT UNIX_TIMESTAMP(`time_start`) FROM `runs` WHERE `id` = %s', run_number)
        if len(result) == 0:
            return 0

        return result[0]

    def _do_get_next_test_id(self): #override
        copy_result = self._mysql.query('SELECT MIN(`id`) FROM `copy_requests`')[0]
        if copy_result == None:
            copy_result = 0

        deletion_result = self._mysql.query('SELECT MIN(`id`) FROM `deletion_requests`')[0]
        if deletion_result == None:
            deletion_result = 0

        return min(copy_result, deletion_result) - 1

    def _make_site_id_map(self):
        self._site_id_map = {}
        for name, site_id in self._mysql.query('SELECT `name`, `id` FROM `sites`'):
            self._site_id_map[name] = int(site_id)

    def _make_dataset_id_map(self):
        self._dataset_id_map = {}
        for name, dataset_id in self._mysql.query('SELECT `name`, `id` FROM `datasets`'):
            self._dataset_id_map[name] = int(dataset_id)

    def _make_replica_map(self, replicas):
        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        indices_to_replicas = {}
        for replica in replicas:
            dataset = replica.dataset
            site = replica.site
            dataset_id = self._dataset_id_map[dataset.name]
            site_id = self._site_id_map[site.name]
            indices_to_replicas[(site_id, dataset_id)] = replica

        return indices_to_replicas
