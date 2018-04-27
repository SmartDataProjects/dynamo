import os
import socket
import logging
import time
import re
import collections
import datetime

from common.interface.history import TransactionHistoryInterface
from common.interface.mysql import MySQL
from common.dataformat import HistoryRecord
import common.configuration as config

logger = logging.getLogger(__name__)

class LocalHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self):
        super(self.__class__, self).__init__()

        self._mysql = MySQL(**config.mysqlhistory.db_params)

        self._site_id_map = {}
        self._dataset_id_map = {}

    def _do_save_dataset_transfers(self,replica_list,replica_times):
        print "will be saving new filled replicas"
        timenow = datetime.datetime.now()
        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        new_datasets = []
        new_sites = []
        for replica in replica_list:
            if replica.dataset.name not in self._dataset_id_map:
                new_datasets.append(replica.dataset.name)
            if replica.site.name not in self._site_id_map:
                new_sites.append(replica.site.name)
        
        if len(new_datasets) > 0:
            self._mysql.insert_many('datasets', ('name',), lambda n: (n,), new_datasets)
            self._make_dataset_id_map()
        if len(new_sites) > 0:
            self._mysql.insert_many('sites', ('name',), lambda n: (n,), new_sites)
            self._make_site_id_map()

        self._mysql.insert_many('copy_dataset', ('item_id', 'site_to','size','created','updated'), 
                                lambda d: (self._dataset_id_map[d.dataset.name],
                                           self._site_id_map[d.site.name],
                                           d.dataset.size,replica_times[d],timenow), replica_list)

    def _do_save_replica_deletions(self,replica_list,replica_times):
        print "will be saving deleted replicas"
        timenow = datetime.datetime.now()
        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        new_datasets = []
        new_sites = []
        for replica in replica_list:
            if replica.dataset.name not in self._dataset_id_map:
                new_datasets.append(replica.dataset.name)
            if replica.site.name not in self._site_id_map:
                new_sites.append(replica.site.name)

        if len(new_datasets) > 0:
            self._mysql.insert_many('datasets', ('name',), lambda n: (n,), new_datasets)
            self._make_dataset_id_map()
        if len(new_sites) > 0:
            self._mysql.insert_many('sites', ('name',), lambda n: (n,), new_sites)
            self._make_site_id_map()

        self._mysql.insert_many('delete_dataset', ('item_id', 'site','size','created','updated'),
                                lambda d: (self._dataset_id_map[d.dataset.name],
                                           self._site_id_map[d.site.name],
                                           d.dataset.size,replica_times[d],timenow), replica_list)

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

    def _do_new_run(self, operation, partition, policy_version, is_test, comment): #override
        part_ids = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(part_ids) == 0:
            self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', partition)
            part_id = self._mysql.last_insert_id
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

        self._mysql.query('INSERT INTO `runs` (`operation`, `partition_id`, `policy_version`, `comment`, `time_start`) VALUES (%s, %s, %s, %s, NOW())', operation_str, part_id, policy_version, comment)

        return self._mysql.last_insert_id

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

        dataset_ids = self._mysql.select_many('datasets', ('id',), 'name', [d.name for d in datasets])

        self._mysql.query('INSERT INTO `deletion_requests` (`id`, `run_id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, %s, NOW(), %s, %s, %s)', operation_id, run_number, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda did: (operation_id, did), dataset_ids)

    def _do_update_copy_entry(self, copy_record): #override
        self._mysql.query('UPDATE `copy_requests` SET `approved` = %s, `size` = %s, `completed` = %s WHERE `id` = %s', copy_record.approved, copy_record.size, copy_record.status, copy_record.operation_id)
        
    def _do_update_deletion_entry(self, deletion_record): #override
        self._mysql.query('UPDATE `deletion_requests` SET `approved` = %s, `size` = %s WHERE `id` = %s', deletion_record.approved, deletion_record.size, deletion_record.operation_id)

    def _do_save_sites(self, run_number, inventory): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        sites_to_insert = []
        for site_name in inventory.sites.keys():
            if site_name not in self._site_id_map:
                sites_to_insert.append(site_name)

        if len(sites_to_insert) != 0:
            self._mysql.insert_many('sites', ('name',), lambda n: (n,), sites_to_insert)
            self._make_site_id_map()        

        sites_in_record = set()

        insert_query = 'INSERT INTO `site_status_snapshots` (`site_id`, `run_id`, `status`) VALUES (%s, {run_number}, %s)'.format(run_number = run_number)

        query = 'SELECT s.`name`, ss.`status`+0 FROM `site_status_snapshots` AS ss INNER JOIN `sites` AS s ON s.`id` = ss.`site_id`'
        query += ' WHERE ss.`run_id` = (SELECT MAX(ss2.`run_id`) FROM `site_status_snapshots` AS ss2 WHERE ss2.`site_id` = ss.`site_id` AND ss2.`run_id` <= %d)' % run_number
        record = self._mysql.query(query)

        sites_in_record = set()

        for site_name, status in record:
            try:
                site = inventory.sites[site_name]
            except KeyError:
                continue

            sites_in_record.add(site)

            if site.status != status:
                self._mysql.query(insert_query, self._site_id_map[site.name], site.status)

        for site in inventory.sites.values():
            if site not in sites_in_record:
                self._mysql.query(insert_query, self._site_id_map[site.name], site.status)

    def _do_get_sites(self, run_number): #override
        partition_id = self._mysql.query('SELECT `partition_id` FROM runs WHERE `id` = %s', run_number)[0]

        query = 'SELECT s.`name`, ss.`status` FROM `site_status_snapshots` AS ss INNER JOIN `sites` AS s ON s.`id` = ss.`site_id`'
        query += ' WHERE ss.`run_id` = (SELECT MAX(ss2.`run_id`) FROM `site_status_snapshots` AS ss2 WHERE ss2.`site_id` = ss.`site_id` AND ss2.`run_id` <= %d)' % run_number
        record = self._mysql.query(query)

        status_map = dict([(site_name, status) for site_name, status in record])

        query = 'SELECT s.`name`, q.`quota` FROM `quota_snapshots` AS q INNER JOIN `sites` AS s ON s.`id` = q.`site_id`'
        query += ' WHERE q.`partition_id` = %d' % partition_id
        query += ' AND q.`run_id` = (SELECT MAX(q2.`run_id`) FROM `quota_snapshots` AS q2 WHERE q2.`partition_id` = %d AND q2.`site_id` = q.`site_id` AND q2.`run_id` <= %d)' % (partition_id, run_number)

        quota_map = dict(self._mysql.query(query))

        sites_dict = {}

        for site_name, status in status_map.items():
            try:
                quota = quota_map[site_name]
            except KeyError:
                quota = 0

            sites_dict[site_name] = (status, quota)

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

    def _do_save_quotas(self, run_number, quotas): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        partition_id = self._mysql.query('SELECT `partition_id` FROM runs WHERE `id` = %s', run_number)[0]

        insert_query = 'INSERT INTO `quota_snapshots` (`site_id`, `partition_id`, `run_id`, `quota`) VALUES (%s, {partition_id}, {run_number}, %s)'.format(partition_id = partition_id, run_number = run_number)

        query = 'SELECT s.`name`, q.`quota` FROM `quota_snapshots` AS q INNER JOIN `sites` AS s ON s.`id` = q.`site_id` WHERE'
        query += ' q.`partition_id` = %d' % partition_id
        query += ' AND q.`run_id` = (SELECT MAX(q2.`run_id`) FROM `quota_snapshots` AS q2 WHERE q2.`partition_id` = %d AND q2.`site_id` = q.`site_id` AND q2.`run_id` <= %d)' % (partition_id, run_number)

        record = self._mysql.query(query)

        sites_in_record = set()

        for site_name, last_quota in record:
            try:
                site, quota = next(item for item in quotas.items() if item[0].name == site_name)
            except StopIteration:
                continue

            sites_in_record.add(site)

            if last_quota != quota:
                self._mysql.query(insert_query, self._site_id_map[site.name], quota)

        for site, quota in quotas.items():
            if site not in sites_in_record:
                self._mysql.query(insert_query, self._site_id_map[site.name], quota)

    def _do_save_conditions(self, policies):
        for policy in policies:
            text = re.sub('\s+', ' ', policy.condition.text)
            ids = self._mysql.query('SELECT `id` FROM `policy_conditions` WHERE `text` LIKE %s', text)
            if len(ids) == 0:
                self._mysql.query('INSERT INTO `policy_conditions` (`text`) VALUES (%s)', text)
                policy.condition_id = self._mysql.last_insert_id
            else:
                policy.condition_id = ids[0]

    def _do_save_copy_decisions(self, run_number, copies): #override
        pass

    def _do_save_deletion_decisions(self, run_number, deleted, kept, protected): #override
        # First save the size snapshots of the replicas, which will be referenced when reconstructing the history.
        # Decisions are saved only if they changed from the last run

        if len(self._site_id_map) == 0:
            self._make_site_id_map()
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        # (site_id, dataset_id) -> replica in inventory
        indices_to_replicas = {}
        for replica in deleted.keys():
            indices_to_replicas[(self._site_id_map[replica.site.name], self._dataset_id_map[replica.dataset.name])] = replica
        for replica in kept.keys():
            indices_to_replicas[(self._site_id_map[replica.site.name], self._dataset_id_map[replica.dataset.name])] = replica
        for replica in protected.keys():
            indices_to_replicas[(self._site_id_map[replica.site.name], self._dataset_id_map[replica.dataset.name])] = replica

        partition_id = self._mysql.query('SELECT `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        # size snapshots
        # size NULL means the replica is deleted
        query = 'SELECT t1.`site_id`, t1.`dataset_id`, t1.`size` FROM `replica_size_snapshots` AS t1'
        query += ' WHERE t1.`partition_id` = %d' % partition_id
        query += ' AND t1.`size` IS NOT NULL'
        query += ' AND t1.`run_id` = ('
        query += '  SELECT MAX(t2.`run_id`) FROM `replica_size_snapshots` AS t2 WHERE t2.`site_id` = t1.`site_id` AND t2.`dataset_id` = t1.`dataset_id`'
        query += '  AND t2.`partition_id` = %d AND t2.`run_id` <= %d' % (partition_id, run_number)
        query += ' )'

        in_record = set()
        insertions = []

        # existing replicas that changed size or disappeared
        for site_id, dataset_id, size in self._mysql.query(query):
            index = (site_id, dataset_id)
            try:
                replica = indices_to_replicas[index]
            except KeyError:
                # this replica is not in the inventory any more
                insertions.append((site_id, dataset_id, None))
                continue

            in_record.add(replica)

            if size != replica.size():
                insertions.append((site_id, dataset_id, replica.size()))

        # new replicas
        for index, replica in indices_to_replicas.items():
            if replica not in in_record:
                insertions.append((index[0], index[1], replica.size()))

        fields = ('site_id', 'dataset_id', 'partition_id', 'run_id', 'size')
        mapping = lambda (site_id, dataset_id, size): (site_id, dataset_id, partition_id, run_number, size)
        self._mysql.insert_many('replica_size_snapshots', fields, mapping, insertions)

        # deletion decisions
        decisions = {}
        for replica, condition_id in deleted.items():
            decisions[replica] = ('delete', condition_id)
        for replica, condition_id in kept.items():
            decisions[replica] = ('keep', condition_id)
        for replica, condition_id in protected.items():
            decisions[replica] = ('protect', condition_id)

        query = 'SELECT dd1.`site_id`, dd1.`dataset_id`, dd1.`decision`, dd1.`matched_condition` FROM `deletion_decisions` AS dd1'
        query += ' INNER JOIN `replica_size_snapshots` AS rs1 ON (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`) = (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`)'
        query += ' WHERE dd1.`partition_id` = %d' % partition_id
        query += ' AND rs1.`size` IS NOT NULL'
        query += ' AND rs1.`run_id` = ('
        query += '  SELECT MAX(rs2.`run_id`) FROM `replica_size_snapshots` AS rs2'
        query += '   WHERE (rs2.`site_id`, rs2.`partition_id`, rs2.`dataset_id`) = (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`)'
        query += '   AND rs2.`partition_id` = %d' % partition_id
        query += '   AND rs2.`run_id` <= %d' % run_number
        query += ' )'
        query += ' AND dd1.`run_id` = ('
        query += '  SELECT MAX(dd2.`run_id`) FROM `deletion_decisions` AS dd2'
        query += '   WHERE (dd2.`site_id`, dd2.`partition_id`, dd2.`dataset_id`) = (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`)'
        query += '   AND dd2.`partition_id` = %d' % partition_id
        query += '   AND dd2.`run_id` <= %d' % run_number
        query += ' )'

        insertions = []

        for site_id, dataset_id, rec_decision, rec_condition_id in self._mysql.query(query):
            replica = indices_to_replicas.pop((site_id, dataset_id))

            decision, condition_id = decisions[replica]

            if decision != rec_decision or condition_id != rec_condition_id:
                insertions.append((site_id, dataset_id, decision, condition_id))

        # replicas with no past decision entries
        for index, replica in indices_to_replicas.items():
            insertions.append(index + decisions[replica])

        fields = ('site_id', 'dataset_id', 'partition_id', 'run_id', 'decision', 'matched_condition')
        mapping = lambda (site_id, dataset_id, decision, condition_id): (site_id, dataset_id, partition_id, run_number, decision, condition_id)
        self._mysql.insert_many('deletion_decisions', fields, mapping, insertions)

        # now fill the cache
        self._fill_snapshot_cache(run_number)

    def _do_get_deletion_decisions(self, run_number, size_only): #override
        self._fill_snapshot_cache(run_number)

        partition_id = self._mysql.query('SELECT `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        if size_only:
            # return {site_name: (protect_size, delete_size, keep_size)}
            volumes = {}
            sites = set()

            query = 'SELECT s.`name`, SUM(r.`size`) * 1.e-12 FROM `replica_snapshot_cache` AS c'
            query += ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`'
            query += ' INNER JOIN `deletion_decisions` AS d ON d.`id` = c.`decision_id`'
            query += ' INNER JOIN `sites` AS s ON s.`id` = r.`site_id`'
            query += ' WHERE c.`run_id` = %d' % run_number
            query += ' AND d.`decision` LIKE %s'
            query += ' GROUP BY r.`site_id`'

            for decision in ['protect', 'delete', 'keep']:
                volumes[decision] = dict(self._mysql.query(query, decision))
                sites.update(set(volumes[decision].keys()))

            self._mysql.query('INSERT INTO `replica_snapshot_cache_usage` VALUES (%s, NOW())', run_number)
                
            product = {}
            for site_name in sites:
                v = {}
                for decision in ['protect', 'delete', 'keep']:
                    try:
                        v[decision] = volumes[decision][site_name]
                    except:
                        v[decision] = 0

                product[site_name] = (v['protect'], v['delete'], v['keep'])

            return product

        else:
            # return {site_name: [(dataset_name, size, decision, reason)]}

            query = 'SELECT s.`name`, d.`name`, r.`size`, l.`decision`, p.`text` FROM `replica_snapshot_cache` AS c'
            query += ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`'
            query += ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`'
            query += ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`'
            query += ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`'
            query += ' INNER JOIN `policy_conditions` AS p ON p.`id` = l.`matched_condition`'
            query += ' WHERE c.`run_id` = %d' % run_number
            query += ' ORDER BY s.`name` ASC, r.`size` DESC'

            product = {}

            _site_name = ''

            for site_name, dataset_name, size, decision, reason in self._mysql.query(query):
                if site_name != _site_name:
                    product[site_name] = []
                    current = product[site_name]
                    _site_name = site_name
                
                current.append((dataset_name, size, decision, reason))

            return product

    def _do_save_dataset_popularity(self, run_number, datasets): #override
        if len(self._dataset_id_map) == 0:
            self._make_dataset_id_map()

        fields = ('run_id', 'dataset_id', 'popularity')
        mapping = lambda dataset: (run_number, self._dataset_id_map[dataset.name], dataset.attr['request_weight'] if 'request_weight' in dataset.attr else 0.)
        self._mysql.insert_many('dataset_popularity_snapshots', fields, mapping, datasets)

    def _do_get_incomplete_copies(self, partition): #override
        query = 'SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`'
        query += ' FROM `copy_requests` AS h'
        query += ' INNER JOIN `runs` AS r ON r.`id` = h.`run_id`'
        query += ' INNER JOIN `partitions` AS p ON p.`id` = r.`partition_id`'
        query += ' INNER JOIN `sites` AS s ON s.`id` = h.`site_id`'
        query += ' WHERE h.`id` > 0 AND p.`name` LIKE \'%s\' AND h.`completed` = 0 AND h.`run_id` > 0' % partition
        history_entries = self._mysql.query(query)
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_COPY, eid, site_name, timestamp = timestamp, approved = approved, size = size)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('copied_replicas', ('copy_id', 'dataset_id'), 'copy_id', id_to_record.keys())

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

    def _do_get_latest_deletion_run(self, partition, before): #override
        result = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(result) == 0:
            return 0

        partition_id = result[0]

        sql = 'SELECT MAX(`id`) FROM `runs` WHERE `partition_id` = %d AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` IN (\'deletion\', \'deletion_test\')' % partition_id
        if before > 0:
            sql += ' AND `id` < %d' % before

        result = self._mysql.query(sql)
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

    def _fill_snapshot_cache(self, run_number):
        if self._mysql.query('SELECT COUNT(*) FROM `replica_snapshot_cache` WHERE `run_id` = %s', run_number)[0] == 0:
            partition_id = self._mysql.query('SELECT `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]
    
            query = 'INSERT INTO `replica_snapshot_cache`'
            query += ' SELECT %d, dd1.`site_id`, dd1.`dataset_id`, rs1.`id`, dd1.`id` FROM `deletion_decisions` AS dd1, `replica_size_snapshots` AS rs1' % run_number
            query += ' WHERE (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`) = (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`)'
            query += ' AND dd1.`partition_id` = %d' % partition_id
            query += ' AND rs1.`size` IS NOT NULL'
            query += ' AND rs1.`run_id` = ('
            query += '  SELECT MAX(rs2.`run_id`) FROM `replica_size_snapshots` AS rs2'
            query += '  WHERE (rs2.`site_id`, rs2.`partition_id`, rs2.`dataset_id`) = (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`)'
            query += '  AND rs2.`partition_id` = %d' % partition_id
            query += '  AND rs2.`run_id` <= %d' % run_number
            query += ' )'
            query += ' AND dd1.`run_id` = ('
            query += '  SELECT MAX(dd2.`run_id`) FROM `deletion_decisions` AS dd2'
            query += '  WHERE (dd2.`site_id`, dd2.`partition_id`, dd2.`dataset_id`) = (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`)'
            query += '  AND dd2.`partition_id` = %d' % partition_id
            query += '  AND dd2.`run_id` <= %d' % run_number
            query += ' )'
    
            self._mysql.query(query)
    
            self._mysql.query('INSERT INTO `replica_snapshot_cache_usage` VALUES (%s, NOW())', run_number)

        num_deleted = self._mysql.query('DELETE FROM `replica_snapshot_cache` WHERE `run_id` NOT IN (SELECT `run_id` FROM `replica_snapshot_cache_usage` WHERE `timestamp` > DATE_SUB(NOW(), INTERVAL 1 WEEK))')
        if num_deleted != 0:
            self._mysql.query('OPTIMIZE TABLE `replica_snapshot_cache`')

        num_deleted = self._mysql.query('DELETE FROM `replica_snapshot_cache_usage` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL 1 WEEK)')
        if num_deleted != 0:
            self._mysql.query('OPTIMIZE TABLE `replica_snapshot_cache_usage`')
