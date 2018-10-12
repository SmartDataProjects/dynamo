from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration

class HistoryDatabase(object):
    """
    Interface to the history database. This is a MySQL-specific implementation, and we actually
    expose the backend database.. Will be a bit tricky to replace the backend when we need to do it.
    What we do with the history DB is very much application specific, so it makes little sense
    to have generic abstract interface to individual actions. The methods of this class are just a
    few of the common operations that are necessary for any history recording.
    """

    # default configuration
    _config = Configuration()

    @staticmethod
    def set_default(config):
        HistoryDatabase._config = Configuration(config)

    def __init__(self, config = None):
        if config is None:
            config = HistoryDatabase._config

        self.db = MySQL(config.db_params)

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value

    def save_users(self, user_list, get_ids = False):
        """
        @param user_list  [(name, dn)]
        """
        if self._read_only:
            if get_ids:
                return [0] * len(user_list)
            else:
                return

        self.db.insert_many('users', ('name', 'dn'), None, user_list, do_update = True)

        if get_ids:
            return self.db.select_many('users', ('id',), 'dn', [u[1] for u in user_list])

    def save_user_services(self, service_names, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(service_names)
            else:
                return

        self.db.insert_many('user_services', ('name',), MySQL.make_tuple, service_names, do_update = True)

        if get_ids:
            return self.db.select_many('user_services', ('id',), 'name', service_names)

    def save_partitions(self, partition_names, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(partition_names)
            else:
                return

        self.db.insert_many('partitions', ('name',), MySQL.make_tuple, partition_names, do_update = True)

        if get_ids:
            return self.db.select_many('partitions', ('id',), 'name', partition_names)

    def save_sites(self, site_names, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(site_names)
            else:
                return

        self.db.insert_many('sites', ('name',), MySQL.make_tuple, site_names, do_update = True)

        if get_ids:
            return self.db.select_many('sites', ('id',), 'name', site_names)

    def save_groups(self, group_names, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(group_names)
            else:
                return

        self.db.insert_many('groups', ('name',), MySQL.make_tuple, group_names, do_update = True)

        if get_ids:
            return self.db.select_many('groups', ('id',), 'name', group_names)

    def save_datasets(self, dataset_names, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(dataset_names)
            else:
                return

        self.db.insert_many('datasets', ('name',), MySQL.make_tuple, dataset_names, do_update = True)

        if get_ids:
            return self.db.select_many('datasets', ('id',), 'name', dataset_names)

    def save_blocks(self, block_list, get_ids = False):
        """
        @param block_list   [(dataset name, block name)]
        """
        if self._read_only:
            if get_ids:
                return [0] * len(block_list)
            else:
                return

        reuse_orig = self.db.reuse_connection
        self.db.reuse_connection = True

        datasets = set(b[0] for b in block_list)

        self.save_datasets(datasets)

        columns = [
            '`dataset` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL',
            '`block` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL'
        ]
        self.db.create_tmp_table('blocks_tmp', columns)
        self.db.insert_many('blocks_tmp', ('dataset', 'block'), None, block_list, db = self.db.scratch_db)

        sql = 'INSERT INTO `blocks` (`dataset_id`, `name`)'
        sql += ' SELECT d.`id`, b.`block` FROM `{scratch}`.`blocks_tmp` AS b'.format(scratch = self.db.scratch_db)
        sql += ' INNER JOIN `datasets` AS d ON d.`name` = b.`dataset`'
        self.db.query(sql)

        if get_ids:
            sql = 'SELECT b.`id` FROM `blocks` AS b'
            sql += ' INNER JOIN (SELECT d.`id` dataset_id, t.`block` block_name FROM `{scratch}`.`blocks_tmp` AS t'.format(scratch = self.db.scratch_db)
            sql += ' INNER JOIN `datasets` AS d ON d.`name` = t.`dataset`) AS j ON (j.`dataset_id`, j.`block_name`) = (b.`dataset_id`, b.`name`)'

            ids = self.db.query(sql)

        self.db.drop_tmp_table('blocks_tmp')
        self.db.reuse_connection = reuse_orig

        if get_ids:
            return ids

    def save_files(self, file_data, get_ids = False):
        if self._read_only:
            if get_ids:
                return [0] * len(file_data)
            else:
                return

        self.db.insert_many('files', ('name', 'size'), None, file_data, do_update = True)

        if get_ids:
            return self.db.select_many('files', ('id',), 'name', [f[0] for f in file_data])
