import logging

from dynamo.utils.interface.mysql import MySQL
from dynamo.history.history import HistoryDatabase
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

        self.registry = MySQL(config.registry)
        self.history = HistoryDatabase(config.get('history', None))

        # we'll be using temporary tables
        self.registry.reuse_connection = True
        self.history.db.reuse_connection = True

        self.optype = optype

        self.dry_run = config.get('dry_run', False)
        if self.dry_run:
            self.history.read_only = True

        self.user_cache = {} # reduce interaction with the authorizer

    def lock(self):
        """
        Lock the registry table for lookup + update workflows.
        """
        if not self.dry_run:
            self.registry.lock_tables()

    def unlock(self):
        if not self.dry_run:
            self.registry.unlock_tables()

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

    def _make_temp_registry_tables(self, items, sites):
        """
        Make temporary tables to be used to constrain request search.
        @param items   List of dataset and block names.
        @param sites   List of site names.
        """

        # Make temporary tables and fill copy_ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('items_tmp', columns)
        columns = ['`site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('sites_tmp', columns)

        if items is not None:
            self.registry.insert_many('items_tmp', ('item',), MySQL.make_tuple, items, db = self.registry.scratch_db)
        if sites is not None:
            self.registry.insert_many('sites_tmp', ('site',), MySQL.make_tuple, sites, db = self.registry.scratch_db)

        columns = [
            '`id` int(10) unsigned NOT NULL AUTO_INCREMENT',
            'PRIMARY KEY (`id`)'
        ]
        self.registry.create_tmp_table('ids_tmp', columns)

        sql = 'INSERT INTO `{db}`.`ids_tmp`'
        sql += ' SELECT r.`id` FROM `{op}_requests` AS r WHERE'
        sql += ' 0 NOT IN (SELECT (`site` IN (SELECT `site` FROM `{op}_request_sites` AS s WHERE s.`request_id` = r.`id`)) FROM `{db}`.`sites_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`item` IN (SELECT `item` FROM `{op}_request_items` AS i WHERE i.`request_id` = r.`id`)) FROM `{db}`.`items_tmp`)'
        self.registry.query(sql.format(db = self.registry.scratch_db, op = self.optype))

        return '`{db}`.`ids_tmp`'.format(db = self.registry.scratch_db)

    def _make_temp_history_tables(self, dataset_ids, block_ids, site_ids):
        """
        Make temporary tables to be used to constrain request search.
        @param dataset_ids   List of dataset ids.
        @param block_ids     List of block ids.
        @param site_ids      List of site ids.
        """

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

        columns = [
            '`id` int(10) unsigned NOT NULL AUTO_INCREMENT',
            'PRIMARY KEY (`id`)'
        ]
        self.history.db.create_tmp_table('ids_tmp', columns)

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

        return '`{db}`.`ids_tmp`'.format(db = self.history.db.scratch_db)

    def _make_registry_constraints(self, authorizer, request_id, statuses, users, items, sites):
        constraints = []

        if request_id is not None:
            constraints.append('r.`id` = %d' % request_id)

        if statuses is not None:
            constraints.append('r.`status` IN ' + MySQL.stringify_sequence(statuses))

        if users is not None:
            user_ids = []
            for user in users:
                result = authorizer.identify_user(name = user)
                if result is not None:
                    user, user_id, dn = result
                    self.user_cache[user_id] = (user, dn)
                    user_ids.append(user_id)

            constraints.append('r.`user_id` IN ' + MySQL.stringify_sequence(user_ids))

        if items is not None or sites is not None:
            temp_table = self._make_temp_registry_tables(items, sites)
            constraints.append('r.`id` IN (SELECT `id` FROM {0})'.format(temp_table))

        if len(constraints) != 0:
            return ' WHERE ' + ' AND '.join(constraints)
        else:
            return ''

    def _make_history_constraints(self, request_id, statuses, users, items, sites):
        if users is not None:
            history_user_ids = self.history.save_users(users, get_ids = True)
        else:
            history_user_ids = None

        if items is not None:
            history_dataset_ids, history_block_ids = self._save_items(items)
        else:
            history_dataset_ids = None
            history_block_ids = None

        if sites is not None:
            history_site_ids = self.history.save_sites(sites, get_ids = True)
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

    def _find_user(self, authorizer, user_id):
        try:
            user, dn = self.user_cache[user_id]
        except KeyError:
            result = authorizer.identify_user(uid = user_id)
            if result is None:
                user = None
                dn = None
            else:
                user, dn = self.user_cache[user_id] = (result[0], result[2])

        return user, dn

        
