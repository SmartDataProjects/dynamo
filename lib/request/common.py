import logging
import time

from dynamo.utils.interface.mysql import MySQL
from dynamo.history.history import HistoryDatabase
from dynamo.registry.registry import RegistryDatabase, CacheDatabase
import dynamo.dataformat as df
from dynamo.dataformat.request import Request, RequestAction

LOG = logging.getLogger(__name__)

class RequestManager(object):
    """
    Manager for external copy and deletion requests made through the web interface.
    Requests are written in registry when they are in new and activated states.
    When moving to terminal states (completed, rejected, cancelled) the records are migrated to history.
    This is a MySQL-specific implementation, but the interface is generic. It should be straightforward
    to abstractify the class if necessary.
    """

    # default config
    _config = df.Configuration()

    @staticmethod
    def set_default(config):
        RequestManager._config = df.Configuration(config)

    def __init__(self, optype, config = None):
        """
        @param optype  'copy' or 'deletion'.
        """
        if config is None:
            config = RequestManager._config

        self.registry = RegistryDatabase(config.get('registry', None))
        self.history = HistoryDatabase(config.get('history', None))
        #self.cache = CacheDatabase(config.get('cache', None))

        # we'll be using temporary tables
        self.registry.db.reuse_connection = True
        self.history.db.reuse_connection = True
        #self.cache.db.reuse_connection = True

        self.optype = optype

        self.set_read_only(config.get('read_only', False))

    def set_read_only(self, value = True):
        self._read_only = value

    def lock(self):
        """
        Lock the registry table for lookup + update workflows.
        """
        if not self._read_only:
            self.registry.db.lock_tables()

    def unlock(self):
        if not self._read_only:
            self.registry.db.unlock_tables()

    def _save_items(self, items):
        """
        Save the items into history.
        @param items          List of dataset and block names.

        @return [dataset id], [block id]
        """
        dataset_names = []
        block_names = []

        for item in items:
            # names are validated already
            try:
                dataset_name, block_name = df.Block.from_full_name(item)
            except df.ObjectError:
                dataset_names.append(item)
            else:
                block_names.append((dataset_name, df.Block.to_real_name(block_name)))

        dataset_ids = self.history.save_datasets(dataset_names, get_ids = True)
        block_ids = self.history.save_blocks(block_names, get_ids = True)

        return dataset_ids, block_ids

    def _get_saved_item_ids(self, items):
        """
        Get the history dataset and block ids from the items list.
        @param items          List of dataset and block names.

        @return [dataset id], [block id]
        """
        dataset_names = []
        block_names = []

        for item in items:
            # names are validated already
            try:
                dataset_name, block_name = df.Block.from_full_name(item)
            except df.ObjectError:
                dataset_names.append(item)
            else:
                block_names.append((dataset_name, df.Block.to_real_name(block_name)))

        dataset_ids = self.history.db.select_many('datasets', 'id', 'name', dataset_names)
        block_ids = self.history.db.select_many('blocks', 'id', 'name', block_names)

        return dataset_ids, block_ids

    def _make_temp_registry_tables(self, items, sites):
        """
        Make temporary tables to be used to constrain request search.
        @param items   List of dataset and block names.
        @param sites   List of site names.
        """

        # Make temporary tables and fill copy_ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.db.create_tmp_table('items_tmp', columns)
        columns = ['`site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.db.create_tmp_table('sites_tmp', columns)

        if items is not None:
            self.registry.db.insert_many('items_tmp', ('item',), MySQL.make_tuple, items, db = self.registry.db.scratch_db)

        LOG.info(sites)

        if sites is not None:
            self.registry.db.insert_many('sites_tmp', ('site',), MySQL.make_tuple, sites, db = self.registry.db.scratch_db)

        columns = [
            '`id` int(10) unsigned NOT NULL AUTO_INCREMENT',
            'PRIMARY KEY (`id`)'
        ]
        self.registry.db.create_tmp_table('ids_tmp', columns)


        sql = 'INSERT INTO `{db}`.`ids_tmp`'
        sql += ' SELECT r.`id` FROM `{op}_requests` AS r WHERE'
        sql += ' 0 NOT IN (SELECT (`site` IN (SELECT `site` FROM `{op}_request_sites` AS s WHERE s.`request_id` = r.`id`)) FROM `{db}`.`sites_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`item` IN (SELECT `item` FROM `{op}_request_items` AS i WHERE i.`request_id` = r.`id`)) FROM `{db}`.`items_tmp`)'
        self.registry.db.query(sql.format(db = self.registry.db.scratch_db, op = self.optype))

        self.registry.db.drop_tmp_table('items_tmp')
        self.registry.db.drop_tmp_table('sites_tmp')

        return '`{db}`.`ids_tmp`'.format(db = self.registry.db.scratch_db)

    def _make_temp_history_tables(self, dataset_ids, block_ids, site_ids):
        """
        Make temporary tables to be used to constrain request search.
        @param dataset_ids   List of dataset ids.
        @param block_ids     List of block ids.
        @param site_ids      List of site ids.
        """

        columns = [
            '`id` int(10) unsigned NOT NULL AUTO_INCREMENT',
            'PRIMARY KEY (`id`)'
        ]
        self.history.db.create_tmp_table('ids_tmp', columns)

        tmp_table_name = '`{db}`.`ids_tmp`'.format(db = self.history.db.scratch_db)

        if (dataset_ids is not None and len(dataset_ids) == 0) or \
                (block_ids is not None and len(block_ids) == 0) or \
                (site_ids is not None and len(site_ids) == 0):
            # temp table must be empty
            return tmp_table_name

        # Make temporary tables and fill ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`id` int(10) unsigned NOT NULL']
        self.history.db.create_tmp_table('datasets_tmp', columns)
        columns = ['`id` bigint(20) unsigned NOT NULL']
        self.history.db.create_tmp_table('blocks_tmp', columns)
        columns = ['`id` int(10) unsigned NOT NULL']
        self.history.db.create_tmp_table('sites_tmp', columns)

        if dataset_ids is not None:
            self.history.db.insert_many('datasets_tmp', ('id',), MySQL.make_tuple, dataset_ids, db = self.history.db.scratch_db)
        if block_ids is not None:
            self.history.db.insert_many('blocks_tmp', ('id',), MySQL.make_tuple, block_ids, db = self.history.db.scratch_db)
        if site_ids is not None:
            self.history.db.insert_many('sites_tmp', ('id',), MySQL.make_tuple, site_ids, db = self.history.db.scratch_db)

        # Explaining the query outwards:
        # SELECT `X_id` FROM `{op}_request_X` WHERE `request_id` = r.`id` -> Full list of X for the request
        # `id` IN (SELECT `X_id` ...) -> 0 or 1
        # SELECT (`id` IN (SELECT `X_id` ...)) FROM tmp.`X_tmp` -> 0s and 1s for all entries in X_tmp
        # 0 NOT IN (SELECT ... FROM tmp.`X_tmp`) -> All entries in X_tmp are contained in {op}_request_X for the specific request

        sql = 'INSERT INTO `{db}`.`ids_tmp`'
        sql += ' SELECT r.`id` FROM `{op}_requests` AS r WHERE'
        sql += ' 0 NOT IN (SELECT (`id` IN (SELECT `site_id` FROM `{op}_request_sites` AS s WHERE s.`request_id` = r.`id`)) FROM `{db}`.`sites_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`id` IN (SELECT `dataset_id` FROM `{op}_request_datasets` AS d WHERE d.`request_id` = r.`id`)) FROM `{db}`.`datasets_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`id` IN (SELECT `block_id` FROM `{op}_request_blocks` AS b WHERE b.`request_id` = r.`id`)) FROM `{db}`.`blocks_tmp`)'
        self.history.db.query(sql.format(db = self.history.db.scratch_db, op = self.optype))

        self.history.db.drop_tmp_table('datasets_tmp')
        self.history.db.drop_tmp_table('blocks_tmp')
        self.history.db.drop_tmp_table('sites_tmp')

        return tmp_table_name

    def _make_registry_constraints(self, request_id, statuses, users, items, sites):
        constraints = []

        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)

        if statuses is not None:
            constraints.append('r.`status` IN ' + MySQL.stringify_sequence(statuses))

        if users is not None:
            constraints.append('r.`user` IN ' + MySQL.stringify_sequence(users))

        if items is not None or sites is not None:
            temp_table = self._make_temp_registry_tables(items, sites)
            constraints.append('r.`id` IN (SELECT `id` FROM {0})'.format(temp_table))

        if len(constraints) != 0:
            return ' WHERE ' + ' AND '.join(constraints)
        else:
            return ''

    def _make_history_constraints(self, request_id, statuses, users, items, sites):
        if users is not None:
            history_user_ids = self.history.db.select_many('users', 'id', 'name', users)
        else:
            history_user_ids = None

        if items is not None:
            history_dataset_ids, history_block_ids = self._get_saved_item_ids(items)
        else:
            history_dataset_ids = None
            history_block_ids = None

        if sites is not None:
            history_site_ids = self.history.db.select_many('sites', 'id', 'name', sites)
        else:
            history_site_ids = None

        constraints = []

        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)

        if statuses is not None:
            constraints.append('r.`status` IN ' + MySQL.stringify_sequence(statuses))

        if users is not None:
            constraints.append('r.`user_id` IN ' + MySQL.stringify_sequence(history_user_ids))

        if items is not None or sites is not None:
            temp_table = self._make_temp_history_tables(history_dataset_ids, history_block_ids, history_site_ids)
            constraints.append('r.`id` IN (SELECT `id` FROM {0})'.format(temp_table))

        if len(constraints) != 0:
            return ' WHERE ' + ' AND '.join(constraints)
        else:
            return ''
