from common.interface.history import TransactionHistoryInterface
from common.dataformat import HistoryRecord

class MySQLHistory(TransactionHistoryInterface):
    """
    Transaction history interface implementation using MySQL as the backend.
    """

    def __init__(self):
        super(self.__class__, self).__init__()

        self._mysql = MySQL(**config.mysqlhistory.db_params)

    def _do_acquire_lock(self): #override
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

            logger.warning('Failed to lock database. Waiting 30 seconds..')

            time.sleep(30)

    def _do_release_lock(self): #override
        self._mysql.query('LOCK TABLES `lock` WRITE')
        self._mysql.query('UPDATE `lock` SET `lock_host` = \'\', `lock_process` = 0 WHERE `lock_host` LIKE %s AND `lock_process` = %s', socket.gethostname(), os.getpid())

        # Did the update go through?
        host, pid = self._mysql.query('SELECT `lock_host`, `lock_process` FROM `lock`')[0]
        self._mysql.query('UNLOCK TABLES')

        if host != '' or pid != 0:
            raise LocalStoreInterface.LockError('Failed to release lock from ' + socket.gethostname() + ':' + str(os.getpid()))

    def _do_make_copy_entry(self, site, operation_id, approved, do_list, size): #override
        """
        Arguments:
          do_list: [(dataset, origin)]
        1. Make sure the destination and origin sites are all in the database.
        2. Insert dataset names that were copied.
        3. Make an entry in the main history table.
        """

        self._mysql.insert_many('sites', ('name',), lambda (d, o): (o.name,), do_list)
        self._mysql.insert_many('sites', ('name',), lambda s: (s.name,), [site])

        site_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `sites`'))

        dataset_names = list(set(d.name for d, o in do_list))
        
        self._mysql.insert_many('datasets', ('name',), lambda name: (name,), dataset_names)
        dataset_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))

        self._mysql.query('INSERT INTO `copy_history` (`id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, NOW(), %s, %s, %s)', operation_id, approved, site_ids[site.name], size)

        self._mysql.insert_many('copied_replicas', ('copy_id', 'dataset_id', 'origin_site_id'), lambda (d, o): (operation_id, dataset_ids[d.name], site_ids[o.name]), do_list)

    def _do_make_deletion_entry(self, site, operation_id, approved, datasets, size): #override
        """
        1. Make sure the site is in the database.
        2. Insert dataset names that were deleted.
        3. Make an entry in the main history table.
        """

        site_ids = self._mysql.query('SELECT `id` FROM `sites` WHERE `name` LIKE %s', site.name)
        if len(site_ids) != 0:
            site_id = site_ids[0]
        else:
            site_id = self._mysql.query('INSERT INTO `sites` (`name`) VALUES (%s)', site.name)

        self._mysql.insert_many('datasets', ('name',), lambda d: (d.name,), datasets)
        dataset_ids = dict(self._mysql.query('SELECT `name`, `id` FROM `datasets`'))

        self._mysql.query('INSERT INTO `deletion_history` (`id`, `timestamp`, `approved`, `site`, `size`) VALUES (%s, NOW(), %s, %s, %s)', deletion_id, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda d: (deletion_id, dataset_ids[d.name]), datasets)

    def _do_update_copy_entry(self, copy_record): #override
        self._mysql.query('UPDATE `copy_history` SET `approved` = %s, completion_time = FROM_UNIXTIME(%d) WHERE `id` = %d', copy_record.approved, copy_record.completion_time, copy_record.operation_id)
        
    def _do_update_deletion_entry(self, deletion_record): #override
        self._mysql.query('UPDATE `deletion_history` SET `approved` = %s, completion_time = FROM_UNIXTIME(%d) WHERE `id` = %d', deletion_record.approved, deletion_record.completion_time, deletion_record.operation_id)

    def _do_get_incomplete_copies(self): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, s.`size` FROM `copy_history` AS h INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`completion_time` LIKE \'0000-00-00 00:00:00\'')
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_COPY, eid, site_name, timestamp = timestamp, approved = approved, size = size)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.query('SELECT `copy_id`, `dataset_id`, `origin_site_id` FROM `copied_replicas` AS c INNER JOIN `copy_history` AS h ON h.`id` = c.`copy_id` WHERE h.`completion_time` LIKE \'0000-00-00 00:00:00\' ORDER BY `copy_id`')

        current_copy_id = 0
        for copy_id, dataset_id, origin_site_id in replicas:
            if copy_id != current_copy_id:
                record = id_to_record[copy_id]
                current_copy_id = copy_id

            record.replicas.append(HistoryRecord.CopiedReplica(dataset_name = id_to_dataset[dataset_id], origin_site_name = id_to_site[origin_site_id]))

        return id_to_record.values()

    def _do_get_incomplete_deletions(self): #override
        history_entries = self._mysql.query('SELECT h.`id`, UNIX_TIMESTAMP(h.`timestamp`), h.`approved`, s.`name`, s.`size` FROM `deletion_history` AS h INNER JOIN `sites` AS s ON s.`id` = h.`site_id` WHERE h.`completion_time` LIKE \'0000-00-00 00:00:00\'')
        
        id_to_record = {}
        for eid, timestamp, approved, site_name, size in history_entries:
            id_to_record[eid] = HistoryRecord(HistoryRecord.OP_DELETE, eid, site_name, timestamp = timestamp, approved = approved, size = size)

        id_to_dataset = dict(self._mysql.query('SELECT `id`, `name` FROM `datasets`'))
        id_to_site = dict(self._mysql.query('SELECT `id`, `name` FROM `sites`'))

        replicas = self._mysql.query('SELECT `deletion_id`, `dataset_id` FROM `deleted_replicas` AS c INNER JOIN `deletion_history` AS h ON h.`id` = c.`deletion_id` WHERE h.`completion_time` LIKE \'0000-00-00 00:00:00\' ORDER BY `deletion_id`')

        current_deletion_id = 0
        for deletion_id, dataset_id in replicas:
            if deletion_id != current_deletion_id:
                record = id_to_record[deletion_id]
                current_deletion_id = deletion_id

            record.replicas.append(HistoryRecord.DeletedReplica(dataset_name = id_to_dataset[dataset_id]))

        return id_to_record.values()
