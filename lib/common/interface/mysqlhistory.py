from common.interface.history import TransactionHistoryInterface

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

        self._mysql.query('INSERT INTO copy_history (`id`, `timestamp`, `approved`, `site_id`, `size`) VALUES (%s, NOW(), %s, %s, %s)', operation_id, approved, site_ids[site.name], size)

        self._mysql.insert_many('copied_replicas', ('copy_id', 'dataset_id', 'origin_site_id'), lambda (d, o): (operation_id, dataset_ids[d.name], site_ids[o.name]), do_list)

    def _do_make_deletion_entry(self, site, operation_id, approved, datasets, size):
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

        self._mysql.query('INSERT INTO deletion_history (`id`, `timestamp`, `approved`, `site`, `size`) VALUES (%s, NOW(), %s, %s, %s)', deletion_id, approved, site_id, size)

        self._mysql.insert_many('deleted_replicas', ('deletion_id', 'dataset_id'), lambda d: (deletion_id, dataset_ids[d.name]), datasets)
