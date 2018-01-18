import time
import logging
import fnmatch

from dynamo.core.persistency import InventoryStore
from dynamo.utils.interface.mysql import MySQL
from dynamo.dataformat import Dataset, Block, File, Site, SitePartition, Group, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class MySQLInventoryStore(InventoryStore):
    """InventoryStore with a MySQL backend."""

    def __init__(self, config):
        InventoryStore.__init__(self, config)

        self._mysql = MySQL(config.db_params)

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

    def load_data(self, inventory, group_names = None, site_names = None, dataset_names = None): #override
        ## Load groups
        LOG.info('Loading groups.')

        # name constraints communicated between _load_* functions via load_tmp tables
        if self._mysql.table_exists('groups_load_tmp'):
            self._mysql.query('DROP TABLE `groups_load_tmp`')

        id_group_map = {0: inventory.groups[None]}
        self._load_groups(inventory, group_names, id_group_map)

        LOG.info('Loaded %d groups.', len(inventory.groups))

        ## Load sites
        LOG.info('Loading sites.')

        if self._mysql.table_exists('sites_load_tmp'):
            self._mysql.query('DROP TABLE `sites_load_tmp`')

        id_site_map = {}
        self._load_sites(inventory, site_names, id_site_map)

        LOG.info('Loaded %d sites.', len(inventory.sites))

        ## Load datasets
        LOG.info('Loading datasets.')
        start = time.time()

        if self._mysql.table_exists('datasets_load_tmp'):
            self._mysql.query('DROP TABLE `datasets_load_tmp`')

        id_dataset_map = {}
        self._load_datasets(inventory, dataset_names, id_dataset_map)

        LOG.info('Loaded %d datasets in %.1f seconds.', len(inventory.datasets), time.time() - start)

        ## Load blocks
        LOG.info('Loading blocks.')
        start = time.time()

        id_block_maps = {} # {dataset_id: {block_id: block}}
        self._load_blocks(inventory, id_dataset_map, id_block_maps)

        num_blocks = sum(len(m) for m in id_block_maps.itervalues())

        LOG.info('Loaded %d blocks in %.1f seconds.', num_blocks, time.time() - start)

        ## Load replicas (dataset and block in one go)
        LOG.info('Loading replicas.')
        start = time.time()

        self._load_replicas(inventory, id_group_map, id_site_map, id_dataset_map, id_block_maps)

        num_dataset_replicas = 0
        num_block_replicas = 0
        for dataset in id_dataset_map.itervalues():
            num_dataset_replicas += len(dataset.replicas)
            num_block_replicas += sum(len(r.block_replicas) for r in dataset.replicas)

        LOG.info('Loaded %d dataset replicas and %d block replicas in %.1f seconds.', num_dataset_replicas, num_block_replicas, time.time() - start)

        ## cleanup
        if self._mysql.table_exists('blocks_load_tmp'):
            self._mysql.query('DROP TABLE `blocks_load_tmp`')
        if self._mysql.table_exists('sites_load_tmp'):
            self._mysql.query('DROP TABLE `sites_load_tmp`')
        if self._mysql.table_exists('datasets_load_tmp'):
            self._mysql.query('DROP TABLE `datasets_load_tmp`')

    def _load_groups(self, inventory, group_names, id_group_map):
        sql = 'SELECT g.`id`, g.`name`, g.`olevel` FROM `groups` AS g'

        if group_names is not None:
            # first dump the group ids into a temporary table, then constrain the original table
            self._mysql.query('CREATE TABLE `groups_load_tmp` (`id` int(11) unsigned NOT NULL, PRIMARY KEY (`id`))')
            sqlbase = 'INSERT INTO `groups_load_tmp` SELECT `id` FROM `groups`'
            self._mysql.execute_many(sqlbase, 'name', group_names)

            sql += ' INNER JOIN `groups_load_tmp` AS t ON t.`id` = g.`id`'

        for group_id, name, olname in self._mysql.xquery(sql):
            if olname == 'Dataset':
                olevel = Dataset
            else:
                olevel = Block

            group = Group(name, olevel)

            inventory.groups[name] = group
            id_group_map[group_id] = group

    def _load_sites(self, inventory, site_names, id_site_map):
        sql = 'SELECT s.`id`, s.`name`, s.`host`, s.`storage_type`+0, s.`backend`, s.`storage`, s.`cpu`, `status`+0 FROM `sites` AS s'

        if site_names is not None:
            # first dump the site ids into a temporary table, then constrain the original table
            self._mysql.query('CREATE TABLE `sites_load_tmp` (`id` int(11) unsigned NOT NULL, PRIMARY KEY (`id`))')
            sqlbase = 'INSERT INTO `sites_load_tmp` SELECT `id` FROM `sites`'
            self._mysql.execute_many(sqlbase, 'name', site_names)

            sql += ' INNER JOIN `sites_load_tmp` AS t ON t.`id` = s.`id`'

        for site_id, name, host, storage_type, backend, storage, cpu, status in self._mysql.xquery(sql):
            site = Site(
                name,
                host = host,
                storage_type = storage_type,
                backend = backend,
                storage = storage,
                cpu = cpu,
                status = status
            )

            inventory.sites[name] = site
            id_site_map[site_id] = site

            for partition in inventory.partitions.itervalues():
                site.partitions[partition] = SitePartition(site, partition)

        # Load site quotas
        sql = 'SELECT q.`site_id`, p.`name`, q.`storage` FROM `quotas` AS q INNER JOIN `partitions` AS p ON p.`id` = q.`partition_id`'
        if site_names is not None:
            sql += ' INNER JOIN `sites_load_tmp` AS t ON t.`id` = q.`site_id`'

        for site_id, partition_name, storage in self._mysql.xquery(sql):
            try:
                site = id_site_map[site_id]
            except KeyError:
                continue

            partition = inventory.partitions[partition_name]
            site.partitions[partition].set_quota(storage * 1.e+12)

    def _load_datasets(self, inventory, dataset_names, id_dataset_map):
        sql = 'SELECT d.`id`, d.`name`, d.`size`, d.`num_files`, d.`status`+0, d.`data_type`+0, s.`cycle`, s.`major`, s.`minor`, s.`suffix`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open`'
        sql += ' FROM `datasets` AS d'
        sql += ' LEFT JOIN `software_versions` AS s ON s.`id` = d.`software_version_id`'

        if dataset_names is not None:
            # first dump the dataset ids into a temporary table, then constrain the original table
            self._mysql.query('CREATE TABLE `datasets_load_tmp` (`id` int(11) unsigned NOT NULL, PRIMARY KEY (`id`))')
            sqlbase = 'INSERT INTO `datasets_load_tmp` SELECT `id` FROM `datasets`'
            self._mysql.execute_many(sqlbase, 'name', dataset_names)

            sql += ' INNER JOIN `datasets_load_tmp` AS t ON t.`id` = d.`id`'

        for dataset_id, name, size, num_files, status, data_type, sw_cycle, sw_major, sw_minor, sw_suffix, last_update, is_open in self._mysql.xquery(sql):
            # size and num_files are reset when loading blocks
            dataset = Dataset(
                name,
                size = size,
                num_files = num_files,
                status = int(status),
                data_type = int(data_type),
                last_update = last_update,
                is_open = (is_open == 1)
            )
            if sw_cycle is None:
                dataset.software_version = None
            else:
                dataset.software_version = (sw_cycle, sw_major, sw_minor, sw_suffix)

            inventory.datasets[name] = dataset
            id_dataset_map[dataset_id] = dataset

    def _load_blocks(self, inventory, id_dataset_map, id_block_maps):
        sql = 'SELECT b.`id`, b.`dataset_id`, b.`name`, b.`size`, b.`num_files`, b.`is_open`, UNIX_TIMESTAMP(b.`last_update`) FROM `blocks` AS b'

        if self._mysql.table_exists('datasets_load_tmp'):
            sql += ' INNER JOIN `datasets_load_tmp` AS t ON t.`id` = b.`dataset_id`'

        sql += ' ORDER BY b.`dataset_id`'

        _dataset_id = 0
        dataset = None
        for block_id, dataset_id, name, size, num_files, is_open, last_update in self._mysql.xquery(sql):
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id

                dataset = id_dataset_map[dataset_id]
                dataset.blocks.clear()
                dataset.size = 0
                dataset.num_files = 0

                id_block_map = id_block_maps[dataset_id] = {}

            block = Block(
                Block.to_internal_name(name),
                dataset,
                size,
                num_files,
                (is_open == 1),
                last_update
            )

            dataset.blocks.add(block)
            dataset.size += block.size
            dataset.num_files += block.num_files

            id_block_map[block_id] = block

    def _load_replicas(self, inventory, id_group_map, id_site_map, id_dataset_map, id_block_maps):
        sql = 'SELECT dr.`dataset_id`, dr.`site_id`,'
        sql += ' br.`block_id`, br.`group_id`, br.`is_complete`, br.`is_custodial`, brs.`size`, UNIX_TIMESTAMP(br.`last_update`)'
        sql += ' FROM `dataset_replicas` AS dr'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`'
        sql += ' INNER JOIN `blocks` AS b ON b.`dataset_id` = d.`id`'
        sql += ' INNER JOIN `block_replicas` AS br ON (br.`block_id`, br.`site_id`) = (b.`id`, dr.`site_id`)'
        sql += ' LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (br.`block_id`, br.`site_id`)'

        if self._mysql.table_exists('groups_load_tmp'):
            sql += ' INNER JOIN `groups_load_tmp` AS gt ON gt.`id` = br.`group_id`'

        if self._mysql.table_exists('sites_load_tmp'):
            sql += ' INNER JOIN `sites_load_tmp` AS st ON st.`id` = dr.`site_id`'

        if self._mysql.table_exists('datasets_load_tmp'):
            sql += ' INNER JOIN `datasets_load_tmp` AS dt ON dt.`id` = dr.`dataset_id`'

        sql += ' ORDER BY dr.`dataset_id`, dr.`site_id`'

        _dataset_id = 0
        _site_id = 0
        dataset_replica = None
        for dataset_id, site_id, block_id, group_id, is_complete, b_is_custodial, b_size, b_last_update in self._mysql.xquery(sql):
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
                    # this is the last dataset_replica
                    # add to dataset and site after filling all block replicas
                    # this does not matter for the dataset, but for the site there is some heavy
                    # computation needed when a replica is added
                    dataset_replica.dataset.replicas.add(dataset_replica)
                    dataset_replica.site.add_dataset_replica(dataset_replica, add_block_replicas = True)

                dataset_replica = DatasetReplica(
                    dataset,
                    site
                )

            block = id_block_map[block_id]
            group = id_group_map[group_id]

            block_replica = BlockReplica(
                block,
                site,
                group = group,
                is_complete = (is_complete == 1),
                is_custodial = (b_is_custodial == 1),
                size = block.size if b_size is None else b_size,
                last_update = b_last_update
            )

            dataset_replica.block_replicas.add(block_replica)
            block.replicas.add(block_replica)

        if dataset_replica is not None:
            # one last bit
            dataset_replica.dataset.replicas.add(dataset_replica)
            dataset_replica.site.add_dataset_replica(dataset_replica, add_block_replicas = True)

    def save_block(self, block): #override
        dataset_id = self._get_dataset_id(block.dataset)
        if dataset_id == 0:
            return

        fields = ('dataset_id', 'name', 'size', 'num_files', 'is_open', 'last_update')
        self._insert_update('blocks', fields, dataset_id, block.real_name(), block.size, block.num_files, block.is_open, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block.last_update)))

    def delete_block(self, block): #override
        dataset_id = self._get_dataset_id(block.dataset)
        if dataset_id == 0:
            return

        sql = 'DELETE FROM `blocks` WHERE `dataset_id` = %s AND `name` = %s'
        self._mysql.query(sql, dataset_id, block.real_name())

    def save_blockreplica(self, block_replica): #override
        block_id = self._get_block_id(block_replica.block)
        if block_id == 0:
            return

        site_id = self._get_site_id(block_replica.site)
        if site_id == 0:
            return

        group_id = self._get_group_id(block_replica.group)
        if group_id == 0:
            return

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial', 'last_update')
        self._insert_update('block_replicas', fields, block_id, site_id, group_id, block_replica.is_complete, block_replica.is_custodial, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block_replica.last_update)))

        if block_replica.size != block_replica.block.size:
            fields = ('block_id', 'site_id', 'size')
            self._insert_update('block_replica_sizes', fields, block_id, site_id, block_replica.size)
        else:
            sql = 'DELETE FROM `block_replica_sizes` WHERE `block_id` = %s AND `site_id` = %s'
            self._mysql.query(sql, block_id, site_id)

    def delete_blockreplica(self, block_replica): #override
        block_id = self._get_block_id(block_replica.block)
        if block_id == 0:
            return

        site_id = self._get_site_id(block_replica.site)
        if site_id == 0:
            return

        sql = 'DELETE FROM `block_replicas` WHERE `block_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, block_id, site_id)

        sql = 'DELETE FROM `block_replica_sizes` WHERE `block_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, block_id, site_id)

    def save_dataset(self, dataset): #override
        if dataset.software_version is None:
            software_version_id = 0
        else:
            sql = 'SELECT `id` FROM `software_versions` WHERE (`cycle`, `major`, `minor`, `suffix`) = (%s, %s, %s, %s)'
            
            result = self._mysql.query(sql, *dataset.software_version)
            if len(result) == 0:
                sql = 'INSERT INTO `software_versions` (`cycle`, `major`, `minor`, `suffix`) VALUES (%s, %s, %s, %s)'
                software_version_id = self._mysql.query(sql, *dataset.software_version)
            else:
                software_version_id = result[0]
            
        fields = ('name', 'size', 'num_files', 'status', 'data_type', 'software_version_id', 'last_update', 'is_open')
        self._insert_update('datasets', fields, dataset.name, dataset.size, dataset.num_files, \
            dataset.status, dataset.data_type, software_version_id,
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dataset.last_update)), dataset.is_open)

    def delete_dataset(self, dataset): #override
        sql = 'DELETE FROM `datasets` WHERE `name` = %s'
        self._mysql.query(sql, dataset.name)

    def save_datasetreplica(self, dataset_replica): #override
        dataset_id = self._get_dataset_id(dataset_replica.dataset)
        if dataset_id == 0:
            return

        site_id = self._get_site_id(dataset_replica.site)
        if site_id == 0:
            return

        fields = ('dataset_id', 'site_id')
        self._insert_update('dataset_replicas', fields, dataset_id, site_id)

    def delete_datasetreplica(self, dataset_replica): #override
        dataset_id = self._get_dataset_id(dataset_replica.dataset)
        if dataset_id == 0:
            return

        site_id = self._get_site_id(dataset_replica.site)
        if site_id == 0:
            return

        sql = 'DELETE FROM `dataset_replicas` WHERE `dataset_id` = %s AND `site_id` = %s'
        self._mysql.query(sql, dataset_id, site_id)

    def save_group(self, group): #override
        fields = ('name', 'olevel')
        self._insert_update('groups', fields, group.name, group.olevel.__name__)

    def delete_group(self, group): #override
        sql = 'DELETE FROM `groups` WHERE `name` = %s'
        self._mysql.query(sql, group.name)

    def save_partition(self, partition): #override
        fields = ('name',)
        self._insert_update('partitions', fields, partition.name)

    def delete_partition(self, partition): #override
        sql = 'DELETE FROM `partitions` WHERE `name` = %s'
        self._mysql.query(sql, partition.name)

    def save_site(self, site): #override
        fields = ('name', 'host', 'storage_type', 'backend', 'storage', 'cpu', 'status')
        self._insert_update('sites', fields, site.name, site.host, site.storage_type, site.backend, site.storage, site.cpu, site.status)

    def delete_site(self, site): #override
        sql = 'DELETE FROM `sites` WHERE `name` = %s'
        self._mysql.query(sql, site.name)

    def save_sitepartition(self, site_partition): #override
        # We are only saving quotas. For superpartitions, there is nothing to do.
        if site_partition.partition.subpartitions is not None:
            return

        site_id = self._get_site_id(site_partition.site)
        if site_id == 0:
            return

        partition_id = self._get_partition_id(site_partition.partition)
        if partition_id == 0:
            return

        fields = ('site_id', 'partition_id', 'storage')
        self._insert_update('quotas', fields, site_id, partition_id, site_partition.quota * 1.e-12)

    def delete_sitepartition(self, site_partition): #override
        # We are only saving quotas. For superpartitions, there is nothing to do.
        if site_partition.partition.subpartitions is not None:
            return

        site_id = self._get_site_id(site_partition.site)
        if site_id == 0:
            return

        partition_id = self._get_partition_id(site_partition.partition)
        if partition_id == 0:
            return

        sql = 'DELETE FROM `quotas` WHERE `site_id` = %s AND `partition_id` = %s'
        self._mysql.query(sql, site_id, partition_id)

    def _insert_update(self, table, fields, *values):
        placeholders = ', '.join(['%s'] * len(fields))

        sql = 'INSERT INTO `%s` (' % table
        sql += ', '.join('`%s`' % f for f in fields)
        sql += ') VALUES (' + placeholders + ')'
        sql += ' ON DUPLICATE KEY UPDATE '
        sql += ', '.join(['`%s`=VALUES(`%s`)' % (f, f) for f in fields])

        self._mysql.query(sql, *values)

    def _get_dataset_id(self, dataset):
        sql = 'SELECT `id` FROM `datasets` WHERE `name` = %s'

        result = self._mysql.query(sql, dataset.name)
        if len(result) == 0:
            # should I raise?
            return 0

        return result[0]

    def _get_block_id(self, block):
        sql = 'SELECT b.`id` FROM `blocks` AS b'
        sql += ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`'
        sql += ' WHERE d.`name` = %s AND b.`name` = %s'

        result = self._mysql.query(sql, block.dataset.name, block.real_name())
        if len(result) == 0:
            return 0

        return result[0]

    def _get_site_id(self, site):
        sql = 'SELECT `id` FROM `sites` WHERE `name` = %s'
        
        result = self._mysql.query(sql, site.name)
        if len(result) == 0:
            return 0

        return result[0]

    def _get_group_id(self, group):
        if group.name is None:
            return 0

        sql = 'SELECT `id` FROM `groups` WHERE `name` = %s'
        
        result = self._mysql.query(sql, group.name)
        if len(result) == 0:
            return 0

        return result[0]

    def _get_partition_id(self, partition):
        sql = 'SELECT `id` FROM `partitions` WHERE `name` = %s'
        
        result = self._mysql.query(sql, partition.name)
        if len(result) == 0:
            return 0

        return result[0]
