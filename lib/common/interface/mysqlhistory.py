import os
import socket
import logging
import time
import collections

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
        self._replica_snapshot_ids = {} # (site, dataset) -> snapshot id

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

    def _do_new_run(self, operation, partition, policy_version, is_test): #override
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

        return self._mysql.query('INSERT INTO `runs` (`operation`, `partition_id`, `policy_version`, `time_start`) VALUES (%s, %s, %s, FROM_UNIXTIME(%s))', operation_str, part_id, policy_version, time.time())

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
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        sites_to_insert = []
        for site_name in inventory.sites.keys():
            if site_name not in self._site_id_map:
                sites_to_insert.append(site_name)

        if len(sites_to_insert) != 0:
            self._mysql.insert_many('sites', ('name',), lambda n: (n,), sites_to_insert)
            self._make_site_id_map()

        update_status = {} #site_name -> status
        keep_status = [] #site_names

        for site_name, active, status in self._mysql.query('SELECT `sites`.`name`, sn.`active`, 0 + sn.`status` FROM `site_status_snapshots` AS sn INNER JOIN `sites` ON `sites`.`id` = sn.`site_id` ORDER BY `run_id` DESC'):
            if site_name in update_status or site_name in keep_status:
                continue

            try:
                site = inventory.sites[site_name]
            except KeyError:
                continue

            if status == site.status and active == site.active:
                keep_status.append(site_name)
            else:
                update_status[site_name] = (active, site.status)

        for site_name, site in inventory.sites.items():
            if site_name not in update_status and site_name not in keep_status:
                update_status[site_name] = (site.active, site.status)

        fields = ('site_id', 'run_id', 'active', 'status')
        mapping = lambda (site_name, (active, status)): (self._site_id_map[site_name], run_number, active, status)
        self._mysql.insert_many('site_status_snapshots', fields, mapping, update_status.items())

    def _do_get_sites(self, run_number): #override
        partition_id = self._mysql.query('SELECT `partition_id` FROM runs WHERE `id` = %s', run_number)[0]

        sites_dict = {}

        query = 'SELECT s.`name`, st.`active`, st.`status` FROM `sites` AS s'
        query += ' INNER JOIN `site_status_snapshots` AS st ON st.`site_id` = s.`id`'
        query += ' WHERE st.`run_id` <= %d' % run_number
        query += ' ORDER BY s.`id`, st.`run_id`'
        status = self._mysql.query(query)

        query = 'SELECT s.`name`, q.`quota` FROM `sites` AS s'
        query += ' INNER JOIN `quota_snapshots` AS q on q.`site_id` = s.`id`'
        query += ' WHERE q.`partition_id` = %d AND q.`run_id` <= %d' % (partition_id, run_number)
        query += ' ORDER BY s.`id`, q.`run_id`'
        quota = self._mysql.query(query)

        for site_name, site_active, site_status in status:
            if site_name in sites_dict:
                continue

            try:
                site_quota = next(q for n, q in quota if n == site_name)
            except StopIteration:
                continue

            sites_dict[site_name] = (site_active, site_status, site_quota)

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

    def _do_save_quotas(self, run_number, partition, quotas, inventory): #override
        if len(self._site_id_map) == 0:
            self._make_site_id_map()

        quota_updates = []

        res = self._mysql.query('SELECT `id` FROM `partitions` WHERE `name` LIKE %s', partition)
        if len(res) == 0:
            return

        partition_id = res[0]
        checked_sites = []

        # find outdated quotas
        result = self._mysql.query('SELECT s.`id`, s.`name`, q.`quota` FROM `quota_snapshots` AS q INNER JOIN `sites` AS s ON s.`id` = q.`site_id` WHERE q.`partition_id` = %s AND q.`run_id` <= %s ORDER BY q.`run_id` DESC', partition_id, run_number)

        for site_id, site_name, stored_quota in result:
            if site_id in checked_sites:
                continue
            
            checked_sites.append(site_id)

            try:
                site = inventory.sites[site_name]
            except KeyError:
                continue

            try:
                quota = quotas[site]
            except KeyError:
                continue

            if stored_quota != quota:
                quota_updates.append((site_id, partition_id, run_number, quota))

        # insert quotas for sites not in the table
        for site, quota in quotas.items():
            site_id = self._site_id_map[site.name]
            if site_id not in checked_sites:
                quota_updates.append((site_id, partition_id, run_number, quota))

        fields = ('site_id', 'partition_id', 'run_id', 'quota')
        self._mysql.insert_many('quota_snapshots', fields, lambda u: u, quota_updates)

    def _do_save_replicas(self, run_number, replicas): #override
        """
        1. Compare the list of sites and datasets in the history database to the list of replicas in the argument.
           If a replica is not found in the database, pass it to new_replicas. If database has an extra entry, pass it to snapshots_to_remove.
        2. Move snapshots in snapshots_to_remove (all past entries) to deleted_replica_snapshots. Insert an entry to this table with the current run number and size 0.
        3. Compare the latest snapshots to the list of replicas in the argument.
           If the sizes differ, add the replica to replicas_to_update. If not, add to replica_snapshot_ids.
        4. Insert updated information to replica_snapshots. Update replica_snapshot_ids.
        """
        # find the latest snapshots for all replicas in record
        self._replica_snapshot_ids = {} # (site, dataset) -> snapshot id, cannot be used across partitions

        # (site_id, dataset_id) -> replica in inventory
        indices_to_replicas = self._make_replica_map(replicas)

        # find new replicas with no snapshots
        new_replicas = {} # index -> (replica, size)

        partition_id = self._mysql.query('SELECT `partition_id` FROM `runs` WHERE `id` = %s', run_number)[0]

        # all snapshotted replicas in the partition with nonzero size
        in_record = self._mysql.query('SELECT DISTINCT `site_id`, `dataset_id` FROM `replica_snapshots` WHERE `run_id` <= %s AND `run_id` IN (SELECT `id` FROM `runs` WHERE `partition_id` = %s) ORDER BY `site_id` ASC, `dataset_id` ASC', run_number, partition_id)
        # replicas in inventory
        current = sorted(indices_to_replicas.keys())

        snapshots_to_remove = []
        num_overlap = 0

        irec = 0
        icur = 0
        while irec != len(in_record) and icur != len(current):
            recidx = tuple(map(int, in_record[irec]))
            curidx = current[icur]

            if recidx < curidx:
                # replica not in the current inventory -> deleted
                snapshots_to_remove.append(recidx)
                irec += 1
            elif recidx > curidx:
                # no snapshot in record -> new replica
                replica = indices_to_replicas[curidx]
                new_replicas[curidx] = (replica, replica.size())
                icur += 1
            else:
                num_overlap += 1
                irec += 1
                icur += 1

        while irec != len(in_record):
            # remaining snapshots must all be deleted
            recidx = tuple(map(int, in_record[irec]))
            snapshots_to_remove.append(recidx)
            irec += 1

        while icur != len(current):
            # remaining replicas are all new
            curidx = current[icur]
            replica = indices_to_replicas[curidx]
            new_replicas[curidx] = (replica, replica.size())
            icur += 1

        # move the entries for deleted replicas
        # first insert the "deletion entry (size 0)" to replica_snapshots so that unique ids are assigned to these entries too
        self._mysql.insert_many('replica_snapshots', ('site_id', 'dataset_id', 'run_id', 'size'), lambda idx: (idx[0], idx[1], run_number, 0), snapshots_to_remove, do_update = False)
        # then move the entire set of entries to deleted_replica_snapshots
        self._mysql.execute_many('INSERT INTO `deleted_replica_snapshots` SELECT * FROM `replica_snapshots`', ('site_id', 'dataset_id'), snapshots_to_remove)
        self._mysql.delete_many('replica_snapshots', ('site_id', 'dataset_id'), snapshots_to_remove)

        # now loop over the snapshots again and find the latest snapshots / update the outdated

        replicas_to_update = {} # index -> (replica, size)

        snapshots = self._mysql.query('SELECT `id`, `site_id`, `dataset_id`, `size` FROM `replica_snapshots` WHERE `run_id` <= %s AND `run_id` IN (SELECT `id` FROM `runs` WHERE `partition_id` = %s) ORDER BY `site_id` ASC, `dataset_id` ASC, `run_id` DESC', run_number, partition_id)

        _index = (0, 0)

        for snapshot_id, site_id, dataset_id, size in snapshots:
            index = (int(site_id), int(dataset_id))
            if index == _index:
                # skipping to next replica
                continue

            _index = index

            replica = indices_to_replicas[index]
            repkey = (replica.site, replica.dataset)
            
            replica_size = replica.size() # passed replica is flagged "partial" if actually partial or not fully in the partition -> will add block replica sizes
            if replica_size != int(size):
                replicas_to_update[index] = (replica, replica_size)
            else:
                self._replica_snapshot_ids[repkey] = int(snapshot_id)

        indices_to_replicas = None

        # append contents of new_replicas
        replicas_to_update.update(new_replicas)
        new_replicas = None

        if len(replicas_to_update) != 0:
            fields = ('site_id', 'dataset_id', 'run_id', 'size')
            mapping = lambda (index, (replica, replica_size)): (index[0], index[1], run_number, replica_size)
    
            self._mysql.insert_many('replica_snapshots', fields, mapping, replicas_to_update.items())
    
            # finally fetch the ids of the snapshots added here
            for snapshot_id, site_id, dataset_id in self._mysql.query('SELECT `id`, `site_id`, `dataset_id` FROM `replica_snapshots` WHERE `run_id` = %s', run_number):
                replica, replica_size = replicas_to_update[(int(site_id), int(dataset_id))]
                self._replica_snapshot_ids[(replica.site, replica.dataset)] = int(snapshot_id)

    def _do_save_copy_decisions(self, run_number, copies): #override
        pass

    def _do_save_deletion_decisions(self, run_number, protected, deleted, kept): #override
        fields = ('run_id', 'snapshot_id', 'decision', 'reason')

        mapping = lambda (rep, reason): (run_number, self._replica_snapshot_ids[(rep.site, rep.dataset)], 'protect', MySQL.escape_string(reason))
        self._mysql.insert_many('deletion_decisions', fields, mapping, protected.items())

        mapping = lambda (rep, reason): (run_number, self._replica_snapshot_ids[(rep.site, rep.dataset)], 'delete', MySQL.escape_string(reason))
        self._mysql.insert_many('deletion_decisions', fields, mapping, deleted.items())

        mapping = lambda (rep, reason): (run_number, self._replica_snapshot_ids[(rep.site, rep.dataset)], 'keep', MySQL.escape_string(reason))
        self._mysql.insert_many('deletion_decisions', fields, mapping, kept.items())

    def _do_get_deletion_decisions(self, run_number, size_only): #override
        if size_only:
            query = 'SELECT s.`name`, d.`decision`, SUM(sn.`size`) * 1.e-12 FROM `deletion_decisions` AS d'
            query += ' INNER JOIN `replica_snapshots` AS sn ON sn.`id` = d.`snapshot_id`'
            query += ' INNER JOIN `sites` AS s ON s.`id` = sn.`site_id`'
            query += ' WHERE d.`run_id` = %d' % run_number
            query += ' GROUP BY s.`id`, d.`decision` ORDER BY s.`id`, d.`decision`'

            result = self._mysql.query(query)

            tmp_dict = collections.defaultdict(dict)

            for site_name, decision, size in result:
                tmp_dict[site_name][decision] = size

            product = {}
            for site_name, tmp_cont in tmp_dict.items():
                for dec in ['protect', 'delete', 'keep']:
                    if dec not in tmp_cont:
                        tmp_cont[dec] = 0

                product[site_name] = (tmp_cont['protect'], tmp_cont['delete'], tmp_cont['keep'])

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

    def _do_get_incomplete_deletions(self): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, h.`size`, h.`size_deleted`, UNIX_TIMESTAMP(h.`last_update`) FROM `deletion_requests` AS h INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`size` != h.`size_deleted`')
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size, size_deleted, last_update in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_DELETE, eid, site_name, timestamp = timestamp, approved = approved, size = size, done = size_deleted, last_update = last_update)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.select_many('deleted_replicas', ('deletion_id', 'dataset_id'), 'deletion_id', ['%d' % i for i in id_to_record.keys()])

        current_deletion_id = 0
        for deletion_id, dataset_id in replicas:
            if deletion_id != current_deletion_id:
                record = id_to_record[deletion_id]
                current_deletion_id = deletion_id

            record.replicas.append(HistoryRecord.DeletedReplica(dataset_name = id_to_dataset[dataset_id]))

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

