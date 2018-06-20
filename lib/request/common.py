from dynamo.utils.interface.mysql import MySQL

class RequestManager(object):
    """
    Requests are written in registry when they are in new and activated states.
    When moving to terminal states (completed, rejected, cancelled) the records are migrated to history.
    """

    def __init__(self, config, optype):
        self.registry = MySQL(config.registry)
        self.history = MySQL(config.history)

        # we'll be using temporary tables
        self.registry.reuse_connection = True
        self.history.reuse_connection = True

        self.optype = optype

        self.dry_run = config.get('dry_run', False)

    def lock(self):
        self.registry.lock_tables()

    def unlock(self):
        self.registry.unlock_tables()

    def save_user(self, caller):
        if not self.dry_run:
            self.history.insert_update('users', ('name', 'dn'), caller.name, caller.dn, update_columns = ('name',))

    def save_sites(self, sites):
        if not self.dry_run:
            self.history.insert_many('sites', ('name',), MySQL.make_tuple, sites)

    def save_items(self, items, dataset_names = [], block_names = []):
        del dataset_names[:]
        del block_names[:]

        block_dataset_names = set()
        for item in items:
            # names are validated already
            try:
                dataset_name, block_name = df.Block.from_full_name(item)
            except df.ObjectError:
                dataset_names.append(item)
            else:
                block_dataset_names.add(dataset_name)
                block_names.append((dataset_name, df.Block.to_real_name(block_name)))

        if not self.dry_run:
            self.history.insert_many('datasets', ('name',), MySQL.make_tuple, dataset_names)
            self.history.insert_many('datasets', ('name',), MySQL.make_tuple, block_dataset_names)

        dataset_id_map = dict(self.history.select_many('datasets', ('name', 'id'), 'name', block_dataset_names))
        # redefine block_names with dataset id
        for i in xrange(len(block_names)):
            dname, bname = block_names[i]
            try:
                dataset_id = dataset_id_map[dname]
            except KeyError:
                # can happen in dry runs
                dataset_id = 0

            block_names[i] = (dataset_id, bname)

        if not self.dry_run:
            self.history.insert_many('blocks', ('dataset_id', 'name'), None, block_names)

    def make_temp_registry_tables(self, items, sites):
        # Make temporary tables and fill copy_ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('items_tmp', columns)
        if items is not None:
            mapping = lambda i: (i,)
            self.registry.insert_many('items_tmp', ('item',), mapping, items, db = self.registry.scratch_db)

        columns = ['`site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('sites_tmp', columns)
        if sites is not None:
            mapping = lambda s: (s,)
            self.registry.insert_many('sites_tmp', ('site',), mapping, sites, db = self.registry.scratch_db)

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

    def make_temp_history_tables(self, dataset_names, block_names, sites):
        # Make temporary tables and fill ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`dataset_id` int(10) unsigned NOT NULL']
        self.history.create_tmp_table('datasets_tmp', columns)
        columns = ['`block_id` bigint(20) unsigned NOT NULL']
        self.history.create_tmp_table('blocks_tmp', columns)

        if dataset_names is not None:
            self.history.insert_select_many('datasets_tmp', ('dataset_id',), 'datasets', ('id',), 'name', dataset_names, db = self.history.scratch_db)
        if block_names is not None:
            self.history.insert_select_many('blocks_tmp', ('block_id',), 'blocks', ('id',), ('dataset_id', 'name'), block_names, db = self.history.scratch_db)

        columns = ['`site_id` int(10) unsigned NOT NULL']
        self.history.create_tmp_table('sites_tmp', columns)

        if sites is not None:
            self.history.insert_select_many('sites_tmp', ('site_id',), 'sites', ('id',), 'name', sites, db = self.history.scratch_db)

        columns = [
            '`id` int(10) unsigned NOT NULL AUTO_INCREMENT',
            'PRIMARY KEY (`id`)'
        ]
        self.history.create_tmp_table('ids_tmp', columns)

        sql = 'INSERT INTO `{db}`.`ids_tmp`'
        sql += ' SELECT r.`id` FROM `{op}_requests` AS r WHERE'
        sql += ' 0 NOT IN (SELECT (`site_id` IN (SELECT `site_id` FROM `{op}_request_sites` AS s WHERE s.`request_id` = r.`id`)) FROM `{db}`.`sites_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`dataset_id` IN (SELECT `dataset_id` FROM `{op}_request_datasets` AS d WHERE d.`request_id` = r.`id`)) FROM `{db}`.`datasets_tmp`)'
        sql += ' AND '
        sql += ' 0 NOT IN (SELECT (`block_id` IN (SELECT `block_id` FROM `{op}_request_blocks` AS b WHERE b.`request_id` = r.`id`)) FROM `{db}`.`blocks_tmp`)'
        self.history.query(sql.format(db = self.history.scratch_db, op = self.optype))
