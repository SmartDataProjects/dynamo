import time
import logging
import fnmatch
import hashlib

from dynamo.core.components.persistency import InventoryStore
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Configuration, Partition, Dataset, Block, File, Site, SitePartition, Group, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class MySQLInventoryStore(InventoryStore):
    """InventoryStore with a MySQL backend."""

    def __init__(self, config):
        InventoryStore.__init__(self, config)

        self._mysql = MySQL(config.db_params)

    def close(self):
        self._mysql.close()

    def check_connection(self): #override
        try:
            self._mysql.query('SELECT COUNT(*) FROM `partitions`')
        except:
            return False

        return True

    def new_handle(self): #override
        config = Configuration(db_params = self._mysql.config())
        return MySQLInventoryStore(config)

    def get_partitions(self, conditions): #override
        partition_names = set(self._mysql.query('SELECT `name` FROM `partitions`'))

        for name in set(conditions.iterkeys()) - partition_names:
            LOG.warning('Creating new partition %s defined in the conditions file.', name)
            self._mysql.query('INSERT INTO `partitions` (`name`) VALUES (%s)', name)

        partitions = {}
        for part_id, name in self._mysql.query('SELECT `id`, `name` FROM `partitions`'):
            try:
                condition = conditions[name]
            except KeyError:
                raise RuntimeError('Condition undefined for partition %s', name)

            if type(condition) is list:
                # this is a superpartition
                partitions[name] = Partition(name, pid = part_id)
            else:
                partitions[name] = Partition(name, condition = condition, pid = part_id)

        # set subpartitions for superpartitions
        for partition in partitions.itervalues():
            if partition._condition is not None:
                continue

            subpartitions = []

            subp_names = conditions[partition.name]
            for name in subp_names:
                subp = partitions[name]
                subp._parent = partition
                subpartitions.append(subp)
                
            partition._subpartitions = tuple(subpartitions)

        # finally return as a list
        return partitions.values()

    def get_group_names(self, include = ['*'], exclude = []): #override
        # Load groups
        group_names = []

        names = self._mysql.xquery('SELECT `name` FROM `groups`')

        for name in names:
            for filt in include:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                continue

            for filt in exclude:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                group_names.append(name)

        return group_names

    def get_site_names(self, include = ['*'], exclude = []): #override
        # Load sites
        site_names = []

        names = self._mysql.xquery('SELECT `name` FROM `sites`')

        for name in names:
            for filt in include:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                continue

            for filt in exclude:
                if fnmatch.fnmatch(name, filt):
                    break
            else:
                # no match
                site_names.append(name)

        return site_names

    def get_dataset_names(self, include = ['*'], exclude = []): #override
        dataset_names = []

        include_patterns = []
        for pattern in include:
            sql = 'SELECT `name` FROM `datasets` WHERE `name` LIKE %s'
            names = self._mysql.xquery(sql, pattern.replace('*', '%').replace('?', '_'))
    
            for name in names:
                for filt in exclude:
                    if fnmatch.fnmatch(name, filt):
                        break
                else:
                    # no match
                    dataset_names.append(name)

        return dataset_names

    def get_files(self, block): #override
        if LOG.getEffectiveLevel() == logging.DEBUG:
            LOG.debug('Loading files for block %s', block.full_name())

        files = set()

        if block.id == 0:
            return files

        sql = 'SELECT `id`, `size`, `name`'
        for algo in File.checksum_algorithms:
            sql += ', `%s`' % algo
        sql += ' FROM `files` WHERE `block_id` = %s'

        for row in self._mysql.xquery(sql, block.id):
            file_id, size, name = row[:3]
            files.add(File(name, block = block, size = size, checksum = row[3:], fid = file_id))

        return files

    def get_file_id(self, lfn): #override
        LOG.debug('Loading file id for LFN %s', lfn)

        sql = 'SELECT `id` FROM `files` WHERE `name` = %s'
        result = self._mysql.query(sql, lfn)

        if len(result) == 0:
            return None

        return result[0]

    def find_block_containing(self, lfn): #override
        sql = 'SELECT d.`name`, b.`name` FROM `files` AS f'
        sql += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        sql += ' WHERE f.`name` = %s'

        result = self._mysql.query(sql, lfn)
        if len(result) == 0:
            return None

        return result[0][0], Block.to_internal_name(result[0][1])

    def load_data(self, inventory, group_names = None, site_names = None, dataset_names = None): #override
        ## We need the temporary tables to stay alive
        reuse_connection_orig = self._mysql.reuse_connection
        self._mysql.reuse_connection = True

        ## Load groups
        LOG.info('Loading groups.')

        if group_names is not None:
            # set up a temporary table to be joined with later queries
            groups_tmp = self._setup_constraints('groups', group_names)
        else:
            groups_tmp = None

        id_group_map = {0: inventory.groups[None]}
        num = self._load_groups(inventory, id_group_map, groups_tmp)

        LOG.info('Loaded %d groups.', num)

        ## Load sites
        LOG.info('Loading sites.')

        if site_names is not None:
            # set up a temporary table to be joined with later queries
            sites_tmp = self._setup_constraints('sites', site_names)
        else:
            sites_tmp = None

        id_site_map = {}
        num = self._load_sites(inventory, id_site_map, sites_tmp)

        LOG.info('Loaded %d sites.', num)

        ## Load datasets
        LOG.info('Loading datasets.')
        start = time.time()

        if dataset_names is not None:
            # set up a temporary table to be joined with later queries
            datasets_tmp = self._setup_constraints('datasets', dataset_names)
        else:
            datasets_tmp = None

        id_dataset_map = {}
        num = self._load_datasets(inventory, id_dataset_map, datasets_tmp)

        LOG.info('Loaded %d datasets in %.1f seconds.', num, time.time() - start)

        ## Load blocks
        LOG.info('Loading blocks.')
        start = time.time()

        id_block_maps = {} # {dataset_id: {block_id: block}}
        num = self._load_blocks(inventory, id_dataset_map, id_block_maps, datasets_tmp)

        num_blocks = sum(len(m) for m in id_block_maps.itervalues())

        LOG.info('Loaded %d blocks in %.1f seconds.', num_blocks, time.time() - start)

        ## Load replicas (dataset and block in one go)
        LOG.info('Loading replicas.')
        start = time.time()

        self._load_replicas(
            inventory, id_group_map, id_site_map, id_dataset_map, id_block_maps,
            groups_tmp, sites_tmp, datasets_tmp
        )

        num_dataset_replicas = 0
        num_block_replicas = 0
        for dataset in id_dataset_map.itervalues():
            num_dataset_replicas += len(dataset.replicas)
            num_block_replicas += sum(len(r.block_replicas) for r in dataset.replicas)

        LOG.info('Loaded %d dataset replicas and %d block replicas in %.1f seconds.', num_dataset_replicas, num_block_replicas, time.time() - start)

        ## Cleanup
        if group_names is not None:
            self._mysql.drop_tmp_table('groups_load')
        if site_names is not None:
            self._mysql.drop_tmp_table('sites_load')
        if dataset_names is not None:
            self._mysql.drop_tmp_table('datasets_load')

        self._mysql.reuse_connection = reuse_connection_orig

    def _load_groups(self, inventory, id_group_map, groups_tmp):
        for group in self._yield_groups(groups_tmp = groups_tmp):
            inventory.groups.add(group)
            id_group_map[group.id] = group

        return len(id_group_map)

    def _load_sites(self, inventory, id_site_map, sites_tmp):
        for site in self._yield_sites(sites_tmp = sites_tmp):
            inventory.sites.add(site)
            id_site_map[site.id] = site

            for partition in inventory.partitions.itervalues():
                site.partitions[partition] = SitePartition(site, partition)

        for sitepartition in self._yield_sitepartitions(sites_tmp = sites_tmp):
            site = inventory.sites[sitepartition.site.name]
            partition = inventory.partitions[sitepartition.partition.name]
            site.partitions[partition].set_quota(sitepartition.quota)

        return len(id_site_map)

    def _load_datasets(self, inventory, id_dataset_map, datasets_tmp):
        for dataset in self._yield_datasets(datasets_tmp = datasets_tmp):
            inventory.datasets.add(dataset)
            id_dataset_map[dataset.id] = dataset

        return len(id_dataset_map)

    def _load_blocks(self, inventory, id_dataset_map, id_block_maps, datasets_tmp):
        _dataset_id = 0
        dataset = None
        for block in self._yield_blocks(id_dataset_map = id_dataset_map, datasets_tmp = datasets_tmp):
            if block.dataset.id != _dataset_id:
                dataset = block.dataset
                _dataset_id = dataset.id
                dataset.blocks.clear()
                id_block_map = id_block_maps[dataset.id] = {}
            
            dataset.blocks.add(block)

            id_block_map[block.id] = block

    def _load_replicas(self, inventory, id_group_map, id_site_map, id_dataset_map, id_block_maps, groups_tmp, sites_tmp, datasets_tmp):
        sql = 'SELECT dr.`dataset_id`, dr.`site_id`, dr.`growing`, dr.`group_id`, br.`block_id`, br.`group_id`,'
        sql += ' br.`is_custodial`, UNIX_TIMESTAMP(br.`last_update`),'
        if BlockReplica._use_file_ids:
            sql += ' br.`is_complete`, f.`id`, f.`size`'
        else:
            sql += ' brf.`num_files`, brf.`size`'
        sql += ' FROM `dataset_replicas` AS dr'
        sql += ' INNER JOIN `blocks` AS b ON b.`dataset_id` = dr.`dataset_id`'
        sql += ' LEFT JOIN `block_replicas` AS br ON (br.`block_id`, br.`site_id`) = (b.`id`, dr.`site_id`)'
        if BlockReplica._use_file_ids:
            sql += ' LEFT JOIN `block_replica_files` AS brf ON (brf.`block_id`, brf.`site_id`) = (b.`id`, dr.`site_id`)'
            sql += ' LEFT JOIN `files` AS f ON f.`id` = brf.`file_id`'
        else:
            sql += ' LEFT JOIN `block_replica_sizes` AS brf ON (brf.`block_id`, brf.`site_id`) = (b.`id`, dr.`site_id`)'

        if groups_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS gt ON gt.`id` = br.`group_id`' % (self._mysql.scratch_db, groups_tmp)

        if sites_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS st ON st.`id` = dr.`site_id`' % (self._mysql.scratch_db, sites_tmp)

        if datasets_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS dt ON dt.`id` = dr.`dataset_id`' % (self._mysql.scratch_db, datasets_tmp)

        sql += ' ORDER BY dr.`dataset_id`, dr.`site_id`, b.`id`'

        # Blocks are left joined -> there will be (# sites) x (# blocks) x (# block files) entries per dataset

        _dataset_id = 0
        _site_id = 0
        _block_id = 0
        file_ids = []
        dataset_replica = None
        block_replica = None
        for row in self._mysql.xquery(sql):
            if BlockReplica._use_file_ids:
                dataset_id, site_id, growing, d_group_id, block_id, b_group_id, b_is_custodial, b_last_update, b_is_complete, file_id, file_size = row
            else:
                dataset_id, site_id, growing, d_group_id, block_id, b_group_id, b_is_custodial, b_last_update, b_num_files, b_size = row

            # everything after d_group_id can be None because of LEFT JOIN

            if dataset_id != _dataset_id:
                _dataset_id = dataset_id

                dataset = id_dataset_map[_dataset_id]
                dataset.replicas.clear()

                id_block_map = id_block_maps[dataset_id]

            if site_id != _site_id:
                _site_id = site_id
                site = id_site_map[site_id]

            if dataset_replica is None or dataset is not dataset_replica.dataset or site is not dataset_replica.site:
                if dataset_replica is not None:
                    # previous dataset_replica
                    # add to dataset and site after filling all block replicas
                    # this does not matter for the dataset, but for the site there is some heavy
                    # computation needed when a replica is added
                    dataset_replica.dataset.replicas.add(dataset_replica)
                    dataset_replica.site.add_dataset_replica(dataset_replica, add_block_replicas = True)

                dataset_replica = DatasetReplica(
                    dataset,
                    site
                )
                if growing != 0:
                    dataset_replica.growing = True
                    dataset_replica.group = id_group_map[d_group_id]

            if block_id != _block_id:
                _block_id = block_id
                if block_id is not None:
                    block = id_block_map[block_id]

            if block_id is None:
                # this dataset replica has no block replicas
                continue

            if block_replica is None or block is not block_replica.block or site is not block_replica.site:
                if BlockReplica._use_file_ids and block_replica is not None:
                    # closing the previous block replica
                    if not block_replica_complete:
                        block_replica.size = block_replica_size
                        block_replica.file_ids = tuple(file_ids)

                    block_replica_size = 0
                    del file_ids[:]

                # creating the new replica

                group = id_group_map[b_group_id]

                block_replica = BlockReplica(
                    block,
                    site,
                    group = group,
                    is_custodial = (b_is_custodial == 1),
                    last_update = b_last_update
                )
                # block_replica created as complete - adjusting size and file_ids later

                if BlockReplica._use_file_ids:
                    block_replica_complete = (b_is_complete == 1)
                elif b_size is not None:
                    block_replica.size = b_size
                    block_replica.file_ids = b_num_files
    
                dataset_replica.block_replicas.add(block_replica)
                block.replicas.add(block_replica)

            if BlockReplica._use_file_ids and file_id is not None:
                block_replica_size += file_size
                file_ids.append(file_id)

        # one last bit

        if dataset_replica is not None:
            dataset_replica.dataset.replicas.add(dataset_replica)
            dataset_replica.site.add_dataset_replica(dataset_replica, add_block_replicas = True)

        if BlockReplica._use_file_ids and block_replica is not None and not block_replica_complete:
            block_replica.size = block_replica_size
            block_replica.file_ids = tuple(file_ids)

    def _setup_constraints(self, table, names):
        tmp_table = table + '_load'
        columns = ['`id` int(11) unsigned NOT NULL', 'PRIMARY KEY (`id`)']
        self._mysql.create_tmp_table(tmp_table)

        # first dump the group ids into a temporary table, then constrain the original table
        sqlbase = 'INSERT INTO `%s`.`%s` SELECT `id` FROM `%s`' % (self._mysql.scratch_db, tmp_table, table)
        self._mysql.execute_many(sqlbase, 'name', names)

        return tmp_table

    def _save_partitions(self, partitions): #override
        if self._mysql.table_exists('partitions_tmp'):
            self._mysql.query('DROP TABLE `partitions_tmp`')

        self._mysql.query('CREATE TABLE `partitions_tmp` LIKE `partitions`')

        fields = ('id', 'name')
        mapping = lambda partition: (partition.id, partition.name)

        num = self._mysql.insert_many('partitions_tmp', fields, mapping, partitions, do_update = False)

        self._mysql.query('DROP TABLE `partitions`')
        self._mysql.query('RENAME TABLE `partitions_tmp` TO `partitions`')

        return num

    def _save_groups(self, groups): #override
        if self._mysql.table_exists('groups_tmp'):
            self._mysql.query('DROP TABLE `groups_tmp``')
            
        self._mysql.query('CREATE TABLE `groups_tmp` LIKE `groups`')

        fields = ('id', 'name', 'olevel')
        mapping = lambda group: (group.id, group.name, Group.olevel_name(group.olevel))

        groups = [g for g in groups if g.name is not None]

        num = self._mysql.insert_many('groups_tmp', fields, mapping, groups, do_update = False)

        self._mysql.query('DROP TABLE `groups`')
        self._mysql.query('RENAME TABLE `groups_tmp` TO `groups`')

        return num

    def _save_sites(self, sites): #override
        if self._mysql.table_exists('sites_tmp'):
            self._mysql.query('DROP TABLE `sites_tmp`')

        self._mysql.query('CREATE TABLE `sites_tmp` LIKE `sites`')

        fields = ('id', 'name', 'host', 'storage_type', 'backend', 'status')
        mapping = lambda site: (site.id, site.name, site.host, Site.storage_type_name(site.storage_type), \
            site.backend, Site.status_name(site.status))

        num = self._mysql.insert_many('sites_tmp', fields, mapping, sites, do_update = False)

        if self._mysql.table_exists('filename_mappings_tmp'):
            self._mysql.query('DROP TABLE `filename_mappings_tmp`')

        self._mysql.query('CREATE TABLE `filename_mappings_tmp` LIKE `filename_mappings`')

        fields = ('site_id', 'protocol', 'chain_id', 'index', 'lfn_pattern', 'pfn_pattern')

        def site_mappings():
            for site in sites:
                for protocol, mapping in site.filename_mapping.iteritems():
                    for chain_id, chain in enumerate(mapping._chains):
                        for idx, (lfn, pfn) in enumerate(chain):
                            yield (site.id, protocol, chain_id, idx, lfn, pfn)

        self._mysql.insert_many('filename_mappings_tmp', fields, None, site_mappings(), do_update = False)

        self._mysql.query('DROP TABLE `sites`')
        self._mysql.query('RENAME TABLE `sites_tmp` TO `sites`')

        self._mysql.query('DROP TABLE `filename_mappings`')
        self._mysql.query('RENAME TABLE `filename_mappings_tmp` TO `filename_mappings`')

        return num

    def _save_sitepartitions(self, sitepartitions): #override
        if self._mysql.table_exists('quotas_tmp'):
            self._mysql.query('DROP TABLE `quotas_tmp`')

        self._mysql.query('CREATE TABLE `quotas_tmp` LIKE `quotas`')

        fields = ('site_id', 'partition_id', 'storage')
        mapping = lambda sp: (sp.site.id, sp.partition.id, sp.quota * 1.e-12)

        def sitepartitions_baseonly():
            # we only save quotas - not interested in superpartitions
            for sitepartition in sitepartitions:
                if sitepartition.partition.subpartitions is None:
                    yield sitepartition

        num = self._mysql.insert_many('quotas_tmp', fields, mapping, sitepartitions_baseonly(), do_update = False)

        self._mysql.query('DROP TABLE `quotas`')
        self._mysql.query('RENAME TABLE `quotas_tmp` TO `quotas`')

        return num

    def _save_datasets(self, datasets): #override
        if self._mysql.table_exists('software_versions_tmp'):
            self._mysql.query('DROP TABLE `software_versions_tmp`')

        self._mysql.query('CREATE TABLE `software_versions_tmp` LIKE `software_versions`')

        if self._mysql.table_exists('datasets_tmp'):
            self._mysql.query('DROP TABLE `datasets_tmp`')

        self._mysql.query('CREATE TABLE `datasets_tmp` LIKE `datasets`')

        fields = ('id', 'name', 'status', 'data_type', 'software_version_id', 'last_update', 'is_open')
        mapping = lambda dataset: (dataset.id, dataset.name, dataset.status, dataset.data_type, \
            dataset._software_version_id, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dataset.last_update)), dataset.is_open)

        software_versions = set()
        def get_dataset():
            for dataset in datasets:
                software_versions.add(dataset.software_version)
                yield dataset

        num = self._mysql.insert_many('datasets_tmp', fields, mapping, get_dataset(), do_update = False)

        fields = ('id',) + Dataset.SoftwareVersion.field_names
        mapping = lambda v: (v.id,) + v.value

        self._mysql.insert_many('software_versions_tmp', fields, mapping, software_versions, do_update = False)

        self._mysql.query('DROP TABLE `datasets`')
        self._mysql.query('RENAME TABLE `datasets_tmp` TO `datasets`')

        self._mysql.query('DROP TABLE `software_versions`')
        self._mysql.query('RENAME TABLE `software_versions_tmp` TO `software_versions`')

        return num

    def _save_blocks(self, blocks): #override
        if self._mysql.table_exists('blocks_tmp'):
            self._mysql.query('DROP TABLE `blocks_tmp`')

        self._mysql.query('CREATE TABLE `blocks_tmp` LIKE `blocks`')

        fields = ('id', 'dataset_id', 'name', 'size', 'num_files', 'is_open', 'last_update')
        mapping = lambda block: (block.id, block.dataset.id, block.real_name(), \
            block.size, block.num_files, block.is_open, \
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block.last_update)))

        num = self._mysql.insert_many('blocks_tmp', fields, mapping, blocks, do_update = False)

        self._mysql.query('DROP TABLE `blocks`')
        self._mysql.query('RENAME TABLE `blocks_tmp` TO `blocks`')

        return num

    def _save_files(self, files): #override
        if self._mysql.table_exists('files_tmp'):
            self._mysql.query('DROP TABLE `files_tmp`')

        self._mysql.query('CREATE TABLE `files_tmp` LIKE `files`')

        fields = ('id', 'block_id', 'size', 'name') + File.checksum_algorithms
        mapping = lambda lfile: (lfile.id, lfile.block.id, lfile.size, lfile.lfn) + lfile.checksum

        num = self._mysql.insert_many('files_tmp', fields, mapping, files, do_update = False)

        self._mysql.query('DROP TABLE `files`')
        self._mysql.query('RENAME TABLE `files_tmp` TO `files`')

        return num

    def _save_dataset_replicas(self, replicas): #override
        if self._mysql.table_exists('dataset_replicas_tmp'):
            self._mysql.query('DROP TABLE `dataset_replicas_tmp`')

        self._mysql.query('CREATE TABLE `dataset_replicas_tmp` LIKE `dataset_replicas`')

        fields = ('dataset_id', 'site_id', 'growing', 'group_id')
        mapping = lambda replica: (replica.dataset.id, replica.site.id, replica.growing, replica.group.id if replica.growing else None)

        num = self._mysql.insert_many('dataset_replicas_tmp', fields, mapping, replicas, do_update = False)

        self._mysql.query('DROP TABLE `dataset_replicas`')
        self._mysql.query('RENAME TABLE `dataset_replicas_tmp` TO `dataset_replicas`')

        return num

    def _save_block_replicas(self, replicas): #override
        if self._mysql.table_exists('block_replicas_tmp'):
            self._mysql.query('DROP TABLE `block_replicas_tmp`')

        self._mysql.query('CREATE TABLE `block_replicas_tmp` LIKE `block_replicas`')

        if BlockReplica._use_file_ids:
            # Fill block_replicas_tmp normally
            # is_complete is only used internally to distinguish empty and full replicas when there are no entries in block_replica_files
            fields = ('block_id', 'site_id', 'group_id', 'is_custodial', 'last_update', 'is_complete')

            mapping = lambda replica: (replica.block.id, replica.site.id, \
                                       replica.group.id, replica.is_custodial, \
                                       time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(replica.last_update)),
                                       replica.is_complete())

            num = self._mysql.insert_many('block_replicas_tmp', fields, mapping, replicas, do_update = False)

            # Fill block_replica_files_tmp
            if self._mysql.table_exists('block_replica_files_tmp'):
                self._mysql.query('DROP TABLE `block_replica_files_tmp`')

            self._mysql.query('CREATE TABLE `block_replica_files_tmp` LIKE `block_replica_files`')

            fields = ('block_id', 'site_id', 'file_id')
    
            def get_filereplica():
                for replica in replicas:
                    if replica.is_complete():
                        continue
    
                    for file_id in replica.file_ids:
                        yield (replica.block.id, replica.site.id, file_id)
    
            self._mysql.insert_many('block_replica_files_tmp', fields, None, get_filereplicas(), do_update = False)

            self._mysql.query('DROP TABLE `block_replica_files`')
            self._mysql.query('RENAME TABLE `block_replica_files_tmp` TO `block_replica_files`')

        else:
            # Add a size column to block_replicas_tmp (speed optimization)
            self._mysql.query('ALTER TABLE `block_replicas_tmp` ADD COLUMN `num_files` int(11) NOT NULL, ADD COLUMN `size` bigint(20) NOT NULL')

            # is_complete is not going to be used when _use_file_ids is False, but we'll save it anyway
            fields = ('block_id', 'site_id', 'group_id', 'is_custodial', 'last_update', 'is_complete', 'num_files', 'size')

            mapping = lambda replica: (replica.block.id, replica.site.id, \
                                       replica.group.id, replica.is_custodial, \
                                       time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(replica.last_update)),
                                       replica.is_complete(), replica.file_ids, replica.size)

            num = self._mysql.insert_many('block_replicas_tmp', fields, mapping, replicas, do_update = False)

            # Use SQL-level operation to fill the sizes_tmp table
            if self._mysql.table_exists('block_replica_sizes_tmp'):
                self._mysql.query('DROP TABLE `block_replica_sizes_tmp`')

            self._mysql.query('CREATE TABLE `block_replica_sizes_tmp` LIKE `block_replica_sizes`')

            sql = 'INSERT INTO `block_replica_sizes_tmp` (`block_id`, `site_id`, `num_files`, `size`)'
            sql += ' SELECT r.`block_id`, r.`site_id`, r.`num_files`, r.`size` FROM `block_replicas_tmp` AS r'
            sql += ' INNER JOIN `blocks` AS b ON b.`id` = r.`block_id`'
            sql += ' WHERE r.`num_files` != b.`num_files` OR r.`size` != b.`size`'
            self._mysql.query(sql)

            self._mysql.query('ALTER TABLE `block_replicas_tmp` DROP COLUMN `num_files`, DROP COLUMN `size`')

            self._mysql.query('DROP TABLE `block_replica_sizes`')
            self._mysql.query('RENAME TABLE `block_replica_sizes_tmp` TO `block_replica_sizes`')

        self._mysql.query('DROP TABLE `block_replicas`')
        self._mysql.query('RENAME TABLE `block_replicas_tmp` TO `block_replicas`')

        return num

    def _clone_from_common_class(self, source): #override
        # Do the closest thing to INSERT SELECT

        tables = ['partitions', 'groups', 'sites', 'quotas', 'software_versions', 'filename_mappings',
                  'datasets', 'blocks', 'files', 'dataset_replicas', 'block_replicas', 'block_replica_files', 'block_replica_sizes']

        for table in tables:
            fields = tuple(row[0] for row in self._mysql.query('SHOW COLUMNS FROM `%s`' % table))
            fields_str = ', '.join('`%s`' % f for f in fields)
            self._mysql.query('TRUNCATE TABLE `%s`' % table)
            rows = source._mysql.xquery('SELECT %s FROM `%s`' % (fields_str, table))
            self._mysql.insert_many(table, fields, None, rows, do_update = False)

    def _yield_partitions(self): #override
        sql = 'SELECT `id`, `name` FROM `partitions`'
        for pid, name in self._mysql.xquery(sql):
            yield Partition(name, pid = part_id)

    def _yield_groups(self, groups_tmp = None): #override
        sql = 'SELECT g.`id`, g.`name`, g.`olevel` FROM `groups` AS g'

        if groups_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS t ON t.`id` = g.`id`' % (self._mysql.scratch_db, groups_tmp)

        for group_id, name, olname in self._mysql.xquery(sql):
            yield Group(
                name,
                olevel = Group.olevel_val(olname),
                gid = group_id
            )

    def _yield_sites(self, sites_tmp = None): #override
        sql = 'SELECT s.`id`, s.`name`, s.`host`, s.`storage_type`+0, s.`backend`, `status`+0 FROM `sites` AS s'

        if sites_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS t ON t.`id` = s.`id`' % (self._mysql.scratch_db, sites_tmp)

        mapping_sql = 'SELECT `protocol`, `chain_id`, `index`, `lfn_pattern`, `pfn_pattern` FROM `filename_mappings` WHERE `site_id` = %s'

        for site_id, name, host, storage_type, backend, status in self._mysql.query(sql):
            site = Site(
                name,
                host = host,
                storage_type = int(storage_type),
                backend = backend,
                status = int(status),
                sid = site_id
            )

            all_chains = {}
            for protocol, chain_id, idx, lfn, pfn in self._mysql.xquery(mapping_sql, site_id):
                try:
                    chains = all_chains[protocol]
                except KeyError:
                    chains = all_chains[protocol] = []

                while len(chains) <= chain_id:
                    chains.append([])

                while len(chains[chain_id]) <= idx:
                    chains[chain_id].append(None) # placeholder

                chains[chain_id][idx] = (lfn, pfn)

            for protocol, chains in all_chains.iteritems():
                site.filename_mapping[protocol] = Site.FileNameMapping(chains)

            yield site

    def _yield_sitepartitions(self, sites_tmp = None): #override
        # Load site quotas
        sql = 'SELECT s.`name`, p.`name`, q.`storage` FROM `quotas` AS q'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = q.`site_id`'
        sql += ' INNER JOIN `partitions` AS p ON p.`id` = q.`partition_id`'

        if sites_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS t ON t.`id` = q.`site_id`' % (self._mysql.scratch_db, sites_tmp)

        for site_name, partition_name, storage in self._mysql.xquery(sql):
            yield SitePartition(Site(site_name), Partition(partition_name), quota = storage * 1.e+12)

    def _yield_datasets(self, datasets_tmp = None): #override
        # load software versions first
        # not COUNT(*) - list can have holes
        maxid = self._mysql.query('SELECT MAX(`id`) FROM `software_versions`')[0]
        if maxid is None: # None: no entries in the table
            Dataset._software_versions_byid = [Dataset.SoftwareVersion(None, 0)]
        else:
            Dataset._software_versions_byid = [Dataset.SoftwareVersion(None, 0)] * (maxid + 1)

        Dataset._software_versions_byvalue = {}

        columns = ', '.join('`%s`' % n for n in (('id',) + Dataset.SoftwareVersion.field_names))
        sql = 'SELECT {columns} FROM `software_versions`'.format(columns = columns)

        for row in self._mysql.xquery(sql):
            vid = row[0]
            value = row[1:]
            version = Dataset.SoftwareVersion(value, vid)
            Dataset._software_versions_byid[vid] = version
            Dataset._software_versions_byvalue[value] = version

        sql = 'SELECT d.`id`, d.`name`, d.`status`+0, d.`data_type`+0,'
        sql += ' d.`software_version_id`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open`'
        sql += ' FROM `datasets` AS d'

        if datasets_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS t ON t.`id` = d.`id`' % (self._mysql.scratch_db, datasets_tmp)

        for dataset_id, name, status, data_type, sw_version_id, last_update, is_open in self._mysql.xquery(sql):
            # size and num_files are reset when loading blocks
            dataset = Dataset(
                name,
                status = int(status),
                data_type = int(data_type),
                last_update = last_update,
                is_open = (is_open == 1),
                did = dataset_id
            )
            dataset._software_version_id = sw_version_id

            yield dataset

    def _yield_blocks(self, id_dataset_map = None, datasets_tmp = None): #override
        sql = 'SELECT b.`id`, d.`id`, d.`name`, b.`name`, b.`size`, b.`num_files`, b.`is_open`, UNIX_TIMESTAMP(b.`last_update`) FROM `blocks` AS b'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'

        if datasets_tmp is not None:
            sql += ' INNER JOIN `%s`.`%s` AS t ON t.`id` = b.`dataset_id`' % (self._mysql.scratch_db, datasets_tmp)

        sql += ' ORDER BY b.`dataset_id`'

        _dataset_id = 0
        dataset = None
        for block_id, dataset_id, dataset_name, name, size, num_files, is_open, last_update in self._mysql.xquery(sql):
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id

                if id_dataset_map is not None:
                    dataset = id_dataset_map[dataset_id]

                else:
                    dataset = Dataset(dataset_name, did = dataset_id),

            yield Block(
                Block.to_internal_name(name),
                dataset,
                size = size,
                num_files = num_files,
                is_open = (is_open == 1),
                last_update = last_update,
                bid = block_id
            )

    def _yield_files(self): #override
        sql = 'SELECT f.`id`, d.`name`, d.`id`, b.`name`, b.`id`, f.`name`, f.`size`'
        for algo in File.checksum_algorithms:
            sql += ', `%s`' % algo
        sql += ' FROM `files`'
        sql += ' INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        sql += ' ORDER BY d.`id`, b.`id`'

        _dataset_id = 0
        _block_id = 0
        dataset = None
        block = None
        for row in self._mysql.xquery(sql):
            file_id, dataset_name, dataset_id, block_name, block_id, lfn, size = row[:7]
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id
                dataset = Dataset(dataset_name, did = dataset_id)

            if block_id != _block_id:
                _block_id = block_id
                block = Block(Block.to_internal_name(block_name), dataset, bid = block_id)

            yield File(lfn, block = block, size = size, checksum = row[7:], fid = file_id)

    def _yield_dataset_replicas(self): #override
        sql = 'SELECT d.`id`, d.`name`, s.`id`, s.`name`, dr.`growing`, g.`id`, g.`name` FROM `dataset_replicas` AS dr'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = dr.`site_id`'
        sql += ' LEFT JOIN `groups` AS g ON g.`id` = dr.`group_id`'
        sql += ' ORDER BY d.`id`, s.`id`'

        sites = {}
        groups = {0: Group.null_group}

        _dataset_id = 0
        dataset = None
        for dataset_id, dataset_name, site_id, site_name, growing, group_id, group_name in self._mysql.xquery(sql):
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id
                dataset = Dataset(dataset_name, did = dataset_id)

            try:
                site = sites[site_id]
            except KeyError:
                site = sites[site_id] = Site(site_name, sid = site_id)

            replica = DatasetReplica(dataset, site)
            if growing != 0:
                replica.growing = True

                if group_name is None:
                    group = Group.null_group
                else:
                    try:
                        group = groups[group_id]
                    except KeyError:
                        group = groups[group_id] = Group(group_name, gid = group_id)

                replica.group = group

            yield replica

    def _yield_block_replicas(self): #override
        sql = 'SELECT b.`id`, b.`name`, b.`size`, d.`id`, d.`name`, s.`id`, s.`name`, g.`name`, br.`group_id`,'
        sql += ' br.`is_custodial`, UNIX_TIMESTAMP(br.`last_update`),'
        if BlockReplica._use_file_ids:
            sql += ' br.`is_complete`, f.`id`, f.`size`'
        else:
            sql += ' brs.`num_files`, brs.`size`'
        sql += ' FROM `block_replicas` AS br'
        sql += ' INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        sql += ' INNER JOIN `sites` AS s ON s.`id` = br.`site_id`'
        sql += ' LEFT JOIN `groups` AS g ON g.`id` = br.`group_id`'
        if BlockReplica._use_file_ids:
            sql += ' LEFT JOIN `block_replica_files` AS brf ON (brf.`block_id`, brf.`site_id`) = (b.`id`, dr.`site_id`)'
            sql += ' LEFT JOIN `files` AS f ON f.`id` = brf.`file_id`'
        else:
            sql += ' LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (b.`id`, dr.`site_id`)'
        sql += ' ORDER BY d.`id`, b.`id`, s.`id`'

        sites = {}
        groups = {0: Group.null_group}

        _dataset_id = 0
        dataset = None
        _block_id = 0
        block = None
        _site_id = 0
        site = None
        _group_id = 0
        group = None
        block_replica = None
        file_ids = []
        for row in self._mysql.xquery(sql):
            if BlockReplica._use_file_ids:
                block_id, block_name, block_size, dataset_id, dataset_name, site_id, site_name, group_name, group_id, is_custodial, last_update, is_complete, file_id, file_size = row
            else:
                block_id, block_name, block_size, dataset_id, dataset_name, site_id, site_name, group_name, group_id, is_custodial, last_update, num_files, size = row

            # group_name and last two columns can be None because of LEFT JOIN

            if dataset_id != _dataset_id:
                _dataset_id = dataset_id
                dataset = Dataset(dataset_name, did = dataset_id)

            if block_id != _block_id:
                _block_id = block_id
                block = Block(Block.to_internal_name(name), dataset, size = block_size, bid = block_id)

            if site_id != _site_id:
                _site_id = site_id
                try:
                    site = sites[site_id]
                except KeyError:
                    site = sites[site_id] = Site(site_name, sid = site_id)

            if group_id != _group_id:
                _group_id = group_id
                try:
                    group = groups[group_id]
                except KeyError:
                    group = groups[group_id] = Group(group_name, gid = group_id)

            if block_replica is None or block is not block_replica.block or site is not block_replica.site:
                if BlockReplica._use_file_ids and block_replica is not None:
                    if not block_replica_complete:
                        block_replica.size = block_replica_size
                        block_replica.file_ids = tuple(file_ids)

                    yield block_replica

                    block_replica_size = 0
                    del file_ids[:]

                block_replica = BlockReplica(
                    block,
                    site,
                    group,
                    is_custodial = (is_custodial != 0),
                    last_update = last_update
                )
                # block_replica created as complete - adjusting size and file_ids later

                if BlockReplica._use_file_ids:
                    block_replica_complete = (is_complete == 1)
                else:
                    if size is not None:
                        block_replica.size = size
                        block_replica.file_ids = num_files
                    
                    # when use_file_ids is false, every iteration of the loop hits here
                    yield block_replica

            if BlockReplica._use_file_ids and file_id is not None:
                block_replica_size += file_size
                file_ids.append(file_id)

        if BlockReplica._use_file_ids and block_replica is not None:
            # all block replicas have been yielded if use_file_ids is False
            # if true, we have one last one to yield
            if not block_replica_complete:
                block_replica.size = block_replica_size
                block_replica.file_ids = tuple(file_ids)

            yield block_replica
            
    def save_block(self, block): #override
        dataset_id = block.dataset.id
        if dataset_id == 0:
            return

        fields = ('dataset_id', 'name', 'size', 'num_files', 'is_open', 'last_update')
        self._mysql.insert_update('blocks', fields, dataset_id, block.real_name(), block.size, block.num_files, block.is_open, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block.last_update)))
        block_id = self._mysql.last_insert_id

        if block_id != 0:
            # new insert
            block.id = block_id

    def delete_block(self, block): #override
        dataset_id = block.dataset.id
        if dataset_id == 0:
            return

        sql = 'DELETE FROM b, f, r, rf, rs USING `blocks` AS b'
        sql += ' LEFT JOIN `files` AS f ON f.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replicas` AS r ON r.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replica_files` AS rf ON rf.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replica_sizes` AS rs ON rs.`block_id` = b.`id`'
        sql += ' WHERE b.`dataset_id` = %s AND b.`name` = %s'

        self._mysql.query(sql, dataset_id, block.real_name())

    def save_file(self, lfile): #override
        dataset_id = lfile.block.dataset.id
        if dataset_id == 0:
            return

        block_id = lfile.block.id
        if block_id == 0:
            return

        fields = ('block_id', 'size', 'name') + File.checksum_algorithms
        self._mysql.insert_update('files', fields, block_id, lfile.size, lfile.lfn, *lfile.checksum)
        file_id = self._mysql.last_insert_id

        if file_id != 0:
            # new insert
            lfile.id = file_id

    def delete_file(self, lfile): #override
        sql = 'DELETE FROM f, brf USING `files` AS f'
        sql += ' LEFT JOIN `block_replica_files` AS brf ON brf.`file_id` = f.`id`'
        sql += ' WHERE f.`name` = %s'
        self._mysql.query(sql, lfile.lfn)

    def save_blockreplica(self, block_replica): #override
        block_id = block_replica.block.id
        if block_id == 0:
            return

        site_id = block_replica.site.id
        if site_id == 0:
            return

        is_complete = block_replica.is_complete()

        fields = ('block_id', 'site_id', 'group_id', 'is_custodial', 'last_update', 'is_complete')
        self._mysql.insert_update('block_replicas', fields, block_id, site_id, \
                                  block_replica.group.id, block_replica.is_custodial, \
                                  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block_replica.last_update)),
                                  is_complete)

        if is_complete or block_replica.file_ids is None:
            # If file_ids is None without is_complete(), it is actually a data corruption.
            # We allow the case instead of crashing in the interest of server stability.
            if BlockReplica._use_file_ids:
                table = 'block_replica_files'
            else:
                table = 'block_replica_sizes'

            sql = 'DELETE FROM `{table}` WHERE `block_id` = %s AND `site_id` = %s'.format(table = table)
            self._mysql.query(sql, block_id, site_id)
        else:
            if BlockReplica._use_file_ids:
                fields = ('block_id', 'site_id', 'file_id')
                mapping = lambda fid: (block_id, site_id, fid)
                self._mysql.insert_many('block_replica_files', fields, mapping, block_replica.file_ids)
            else:
                fields = ('block_id', 'site_id', 'num_files', 'size')
                self._mysql.insert_update('block_replica_sizes', fields, block_id, site_id, block_replica.file_ids, block_replica.size)

    def delete_blockreplica(self, block_replica): #override
        dataset_id = block_replica.block.dataset.id
        if dataset_id == 0:
            return

        block_id = block_replica.block.id
        if block_id == 0:
            return

        site_id = block_replica.site.id
        if site_id == 0:
            return

        sql = 'DELETE FROM `block_replicas` WHERE `block_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, block_id, site_id)

        sql = 'DELETE FROM `block_replica_files` WHERE `block_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, block_id, site_id)

        sql = 'DELETE FROM `block_replica_sizes` WHERE `block_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, block_id, site_id)

        sql = 'SELECT COUNT(*) FROM `block_replicas` AS br'
        sql += ' INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`'
        sql += ' WHERE b.`dataset_id` = %s AND br.`site_id` = %s'
        if self._mysql.query(sql, dataset_id, site_id)[0] == 0:
            sql = 'DELETE FROM `dataset_replicas` WHERE `dataset_id` = %s AND `site_id` = %s'
            self._mysql.query(sql, dataset_id, site_id)

    def save_dataset(self, dataset): #override
        if dataset.software_version is not None and dataset._software_version_id != 0:
            sql = 'SELECT COUNT(*) FROM `software_versions` WHERE `id` = %s'
            known_id = (self._mysql.query(sql, dataset._software_version_id)[0] == 1)
            if not known_id:
                columns = ', '.join('`%s`' % n for n in (('id',) + Dataset.SoftwareVersion.field_names))
                placeholders = ', '.join(['%s'] * (1 + len(Dataset.SoftwareVersion.field_names)))
                sql = 'INSERT INTO `software_versions` ({columns}) VALUES ({placeholders})'.format(columns = columns, placeholders = placeholders)
                software_version_id = self._mysql.query(sql, dataset._software_version_id, *dataset.software_version)
            
        fields = ('name', 'status', 'data_type', 'software_version_id', 'last_update', 'is_open')
        self._mysql.insert_update('datasets', fields, dataset.name, \
            dataset.status, dataset.data_type, dataset._software_version_id,
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dataset.last_update)), dataset.is_open)
        dataset_id = self._mysql.last_insert_id

        if dataset_id != 0:
            # new insert
            dataset.id = dataset_id

    def delete_dataset(self, dataset): #override
        sql = 'DELETE FROM d, b, f, dr, br, brf, brs USING `datasets` AS d'
        sql += ' LEFT JOIN `blocks` AS b ON b.`dataset_id` = d.`id`'
        sql += ' LEFT JOIN `files` AS f ON f.`block_id` = b.`id`'
        sql += ' LEFT JOIN `dataset_replicas` AS dr ON dr.`dataset_id` = d.`id`'
        sql += ' LEFT JOIN `block_replicas` AS br ON br.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replica_files` AS brf ON brf.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replica_sizes` AS brs ON brs.`block_id` = b.`id`'
        sql += ' WHERE d.`name` = %s'

        self._mysql.query(sql, dataset.name)

    def save_datasetreplica(self, dataset_replica): #override
        dataset_id = dataset_replica.dataset.id
        if dataset_id == 0:
            return

        site_id = dataset_replica.site.id
        if site_id == 0:
            return

        fields = ('dataset_id', 'site_id', 'growing', 'group_id')
        self._mysql.insert_update('dataset_replicas', fields, dataset_id, site_id, dataset_replica.growing, dataset_replica.group.id if dataset_replica.growing else None)

    def delete_datasetreplica(self, dataset_replica): #override
        dataset_id = dataset_replica.dataset.id
        if dataset_id == 0:
            return

        site_id = dataset_replica.site.id
        if site_id == 0:
            return

        sql = 'DELETE FROM br, brf, brs USING `blocks` AS b'
        sql += ' INNER JOIN `block_replicas` AS br ON br.`block_id` = b.`id`'
        sql += ' LEFT JOIN `block_replica_files` AS brf ON brf.`block_id` = b.`id` AND brf.`site_id` = br.`site_id`'
        sql += ' LEFT JOIN `block_replica_sizes` AS brs ON brs.`block_id` = b.`id` AND brs.`site_id` = br.`site_id`'
        sql += ' WHERE b.`dataset_id` = %s AND br.`site_id` = %s'

        self._mysql.query(sql, dataset_id, site_id)

        sql = 'DELETE FROM `dataset_replicas` WHERE `dataset_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, dataset_id, site_id)

    def save_group(self, group): #override
        fields = ('name', 'olevel')
        self._mysql.insert_update('groups', fields, group.name, Group.olevel_name(group.olevel))
        group_id = self._mysql.last_insert_id

        if group_id != 0:
            group.id = group_id

    def delete_group(self, group): #override
        sql = 'DELETE FROM `groups` WHERE `id` = %s'
        self._mysql.query(sql, group.id)

        sql = 'UPDATE `block_replicas` SET `group_id` = 0 WHERE `group_id` = %s'
        self._mysql.query(sql, group.id)

    def save_partition(self, partition): #override
        fields = ('name',)
        self._mysql.insert_update('partitions', fields, partition.name)
        partition_id = self._mysql.last_insert_id

        if partition_id != 0:
            partition.id = partition_id

        # For new partitions, persistency requires saving site partition data with default parameters.
        # We handle missing site partition entries at load time - if a row is missing, SitePartition object with
        # default parameters will be created.

    def delete_partition(self, partition): #override
        sql = 'DELETE FROM p, q USING `partitions` AS p'
        sql += ' LEFT JOIN `quotas` AS q ON q.`partition_id` = p.`id`'
        sql += ' WHERE p.`name` = %s'
        self._mysql.query(sql, partition.name)

    def save_site(self, site): #override
        fields = ('name', 'host', 'storage_type', 'backend', 'status')
        self._mysql.insert_update('sites', fields, site.name, site.host, site.storage_type, site.backend, site.status)
        site_id = self._mysql.last_insert_id

        if site_id != 0:
            site.id = site_id

        self._mysql.query('DELETE FROM `filename_mappings` WHERE `site_id` = %s', site_id)

        fields = ('site_id', 'protocol', 'chain_id', 'index', 'lfn_pattern', 'pfn_pattern')

        def filename_mappings():
            for protocol, mapping in site.filename_mapping.iteritems():
                for chain_id, chain in enumerate(mapping._chains):
                    for idx, (lfn, pfn) in enumerate(chain):
                        yield (site.id, protocol, chain_id, idx, lfn, pfn)

        self._mysql.insert_many('filename_mappings', fields, None, filename_mappings(), do_update = False)

        # For new sites, persistency requires saving site partition data with default parameters.
        # We handle missing site partition entries at load time - if a row is missing, SitePartition object with
        # default parameters will be created.

    def delete_site(self, site): #override
        sql = 'DELETE FROM s, m, dr, br, brf, brs, q USING `sites` AS s'
        sql += ' LEFT JOIN `filename_mappings` AS m ON m.`site_id` = s.`id`'
        sql += ' LEFT JOIN `dataset_replicas` AS dr ON dr.`site_id` = s.`id`'
        sql += ' LEFT JOIN `block_replicas` AS br ON br.`site_id` = s.`id`'
        sql += ' LEFT JOIN `block_replica_files` AS brf ON brf.`site_id` = s.`id`'
        sql += ' LEFT JOIN `block_replica_sizes` AS brs ON brs.`site_id` = s.`id`'
        sql += ' LEFT JOIN `quotas` AS q ON q.`site_id` = s.`id`'
        sql += ' WHERE s.`name` = %s'
        self._mysql.query(sql, site.name)

    def save_sitepartition(self, site_partition): #override
        # We are only saving quotas. For superpartitions, there is nothing to do.
        if site_partition.partition.subpartitions is not None:
            return

        site_id = site_partition.site.id
        if site_id == 0:
            return

        partition_id = site_partition.partition.id
        if partition_id == 0:
            return

        fields = ('site_id', 'partition_id', 'storage')
        self._mysql.insert_update('quotas', fields, site_id, partition_id, site_partition.quota * 1.e-12)

    def version(self): #override
        """
        Concatenate hex checksums of all tables and take the md5.
        """
        csstr = ''
        for table in ['block_replica_files', 'block_replica_sizes', 'block_replicas', 'blocks', 'dataset_replicas', 'datasets', 'files', 'groups', 'partitions', 'quotas', 'sites', 'filename_mappings', 'software_versions']:
            cksum = hex(self._mysql.query('CHECKSUM TABLE `%s`' % table)[0][1])[2:] # remote 0x
            if len(cksum) < 8:
                padding = '0' * (8 - len(cksum))
                cksum = padding + cksum
            csstr += cksum

        return hashlib.md5(csstr).hexdigest()
