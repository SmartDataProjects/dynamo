import re
import fnmatch

from dynamo.web.exceptions import MissingParameter, ExtraParameter, IllFormedRequest, InvalidRequest
from dynamo.web.modules._userdata import UserDataMixin

class ParseInputMixin(UserDataMixin):
    def __init__(self, config):
        UserDataMixin.__init__(self, config)

        # Parsed and formatted HTTP queries
        self.request = {}

        # Existing request objects, to be filled by fill_from_sql in subclasses
        # {request_id: request object}
        self.existing = {}

        # {name: (id, dn), id: (name, dn)}
        self.user_info_cache = {}

    def parse_input(self, request, inventory, allowed_fields, required_fields = tuple()):
        # Check we have the right request fields

        input_fields = set(request.keys())
        allowed_fields = set(allowed_fields)
        excess = input_fields - allowed_fields
        if len(excess) != 0:
            raise ExtraParameter(list(excess)[0])

        for key in required_fields:
            if key not in request:
                raise MissingParameter(key)

        # Pick up the values and cast them to correct types

        for key in ['request_id', 'n']:
            if key not in request:
                continue

            try:
                self.request[key] = int(request[key])
            except ValueError:
                raise IllFormedRequest(key, request[key], hint = '%s must be an integer' % key)

        for key in ['item', 'status', 'site', 'user']:
            if key not in request:
                continue

            value = request[key]
            if type(value) is str:
                self.request[key] = value.strip().split(',')
            elif type(value) is list:
                self.request[key] = value
            else:
                raise IllFormedRequest(key, request[key], hint = '%s must be a string or a list' % key)

        for key in ['group']:
            if key not in request:
                continue

            self.request[key] = request[key]

        # Check value validity
        # We check the site, group, and item names but not use their ids in the table.
        # The only reason for this would be to make the registry not dependent on specific inventory store technology.

        if 'item' in self.request:
            for item in self.request['item']:
                if item in inventory.datasets:
                    # OK this is a known dataset
                    continue
    
                try:
                    dataset_name, block_name = df.Block.from_full_name(item)
                except df.ObjectError:
                    raise InvalidRequest('Invalid item name %s' % item)
    
                try:
                    inventory.datasets[dataset_name].find_block(block_name, must_find = True)
                except:
                    raise InvalidRequest('Invalid block name %s' % item)

        if 'site' in self.request:
            for site in list(self.request['site']):
                # Wildcard allowed
                if '*' in site or '?' in site or '[' in site:
                    self.request['site'].remove(site)
                    pattern = re.compile(fnmatch.translate(site))

                    for sname in inventory.sites.iterkeys():
                        if pattern.match(sname):
                            self.request['site'].append(sname)
                else:
                    try:
                        inventory.sites[site]
                    except KeyError:
                        raise InvalidRequest('Invalid site name %s' % site)

            if len(self.request['site']) == 0:
                self.request.pop('site')

        if 'group' in self.request:
            try:
                inventory.groups[self.request['group']]
            except KeyError:
                raise InvalidRequest('Invalid group name %s' % self.request['group'])

        # Minor security concern: do we want to expose the user list this way?
        if 'user' in self.request:
            for uname in self.request['user']:
                result = self.authorizer.identify_user(name = uname)
                if result is None:
                    raise InvalidRequest('Invalid user name %s' % uname)

                self.user_info_cache[uname] = result[1:] # id, dn
                self.user_info_cache[result[1]] = (uname, result[2]) # name, dn

        if 'status' in self.request:
            for status in self.request['status']:
                if status not in ('new', 'activated', 'completed', 'rejected', 'cancelled'):
                    raise InvalidRequest('Invalid status value %s' % status)

    def load_existing(self, by_id = False):
        """
        Find an existing request from values in self.request and set self.existing.
        """
        constraints = {}
        if 'request_id' in self.request:
            constraints['request_id'] = self.request['request_id']

        if not by_id:
            if 'status' in self.request:
                constraints['status'] = self.request['status']
    
            if 'user' in self.request:
                constraints['user'] = self.request['user']
    
            if 'item' in self.request:
                constraints['item'] = self.request['item']

            if 'site' in self.request:
                constraints['site'] = self.request['site']

        self.existing = self.fill_from_sql(**constraints)

    def fill_from_sql(self, request_id = None, status = None, user = None, item = None, site = None):
        raise NotImplementedError('fill_from_sql')


class SaveParamsMixin(object):
    def __init__(self, config):
        self.history_dataset_names = []
        self.history_block_names = []

    def save_params(self, caller = None):
        if 'group' in self.requests:
            self.history.insert_update('groups', ('name',), self.request['group'], update_columns = ('name',))

        if caller is not None:
            self.history.insert_update('users', ('name', 'dn'), caller.name, caller.dn, update_columns = ('name',))
        if 'site' in self.request:
            self.history.insert_many('sites', ('name',), MySQL.make_tuple, self.request['site'])

        if 'item' in self.request:
            dataset_names = []
            block_dataset_names = []
            block_names = []
            for item in self.request['item']:
                # names are validated already
                try:
                    dataset_name, block_name = df.Block.from_full_name(item)
                except df.ObjectError:
                    dataset_names.append(item)
                else:
                    block_dataset_names.append(dataset_name)
                    block_names.append((dataset_name, df.Block.to_real_name(block_name)))
    
            self.history.insert_many('datasets', ('name',), MySQL.make_tuple, dataset_names)
            self.history.insert_many('blocks', ('name',), MySQL.make_tuple, block_dataset_names)
    
            dataset_id_map = dict(self.history.select_many('datasets', ('name', 'id'), 'name', block_dataset_names))
            # redefine block_names with dataset id
            for i in xrange(len(block_names)):
                dname, bname = block_names[i]
                block_names[i] = (dataset_id_map[dname], bname)
    
            self.history.insert_many('blocks', ('dataset_id', 'name'), None, block_names)
    
            self.history_dataset_names = dataset_names
            self.history_block_names = block_names

    def make_temp_registry_tables(self, optype, item, site):
        # Make temporary tables and fill copy_ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('items_tmp', columns)
        if item is not None:
            mapping = lambda i: (i,)
            self.registry.insert_many('items_tmp', ('item',), mapping, item, db = self.registry.scratch_db)

        columns = ['`site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL']
        self.registry.create_tmp_table('sites_tmp', columns)
        if site is not None:
            mapping = lambda s: (s,)
            self.registry.insert_many('sites_tmp', ('site',), mapping, site, db = self.registry.scratch_db)

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
        self.registry.query(sql.format(db = self.registry.scratch_db, op = optype))

    def make_temp_history_tables(self, optype, item, site):
        # Make temporary tables and fill ids_tmp with ids of requests whose item and site lists fully cover the provided list of items and sites.
        columns = ['`dataset_id` int(10) unsigned NOT NULL']
        self.history.create_tmp_table('datasets_tmp', columns)
        columns = ['`block_id` bigint(20) unsigned NOT NULL']
        self.history.create_tmp_table('blocks_tmp', columns)

        if item:
            if len(self.history_dataset_names) != 0
                self.history.insert_select_many('datasets_tmp', ('dataset_id',), 'datasets', ('id',), 'name', self.history_dataset_names, db = self.history.scratch_db)
            if len(self.history_block_names) != 0:
                self.history.insert_select_many('blocks_tmp', ('block_id',), 'blocks', ('id',), ('dataset_id', 'name'), self.history_block_names, db = self.history.scratch_db)

        columns = ['`site_id` int(10) unsigned NOT NULL']
        self.history.create_tmp_table('sites_tmp', columns)

        if site is not None:
            self.history.insert_select_many('sites_tmp', ('site_id',), 'sites', ('id',), 'name', site, db = self.history.scratch_db)

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
        self.history.query(sql.format(db = self.history.scratch_db, op = optype))
