import os
import time
import datetime
import re
import socket
import logging
import fnmatch
import pprint

from common.configuration import common_config
from core.persistency import InventoryStore
from common.interface.mysql import MySQL
from dataformat import Dataset, Block, File, Site, SitePartition, Group, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class MySQLInventoryStore(InventoryStore):
    """InventoryPersistency based on MySQL."""

    def __init__(self, config):
        super(self.__class__, self).__init__(config)

        db_params = common_config.mysql
        if 'db_params' in config:
            db_params.update(config['db_params'])

        self._mysql = MySQL(**db_params)

    def get_last_update(self): #override
        return self._mysql.query('SELECT UNIX_TIMESTAMP(`last_update`) FROM `system`')[0]

    def set_last_update(self, tm = -1): #override
        if tm < 0:
            tm = time.time()

        self._mysql.query('UPDATE `system` SET `last_update` = FROM_UNIXTIME(%d)' % int(tm))

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

        id_group_map = {0: None}
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
            site.partitions[partition].set_quota(storage)

    def _load_datasets(self, inventory, dataset_names, id_dataset_map):
        sql = 'SELECT d.`id`, d.`name`, d.`size`, d.`num_files`, d.`status`+0, d.`on_tape`, d.`data_type`+0, s.`cycle`, s.`major`, s.`minor`, s.`suffix`, UNIX_TIMESTAMP(d.`last_update`), d.`is_open`'
        sql += ' FROM `datasets` AS d'
        sql += ' LEFT JOIN `software_versions` AS s ON s.`id` = d.`software_version_id`'

        if dataset_names is not None:
            # first dump the dataset ids into a temporary table, then constrain the original table
            self._mysql.query('CREATE TABLE `datasets_load_tmp` (`id` int(11) unsigned NOT NULL, PRIMARY KEY (`id`))')
            sqlbase = 'INSERT INTO `datasets_load_tmp` SELECT `id` FROM `datasets`'
            self._mysql.execute_many(sqlbase, 'name', dataset_names)

            sql += ' INNER JOIN `datasets_load_tmp` AS t ON t.`id` = d.`id`'

        for dataset_id, name, size, num_files, status, on_tape, data_type, sw_cycle, sw_major, sw_minor, sw_suffix, last_update, is_open in self._mysql.xquery(sql):
            # size and num_files are reset when loading blocks
            dataset = Dataset(
                name,
                size = size,
                num_files = num_files,
                status = int(status),
                on_tape = on_tape,
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
        sql = 'SELECT b.`id`, b.`dataset_id`, b.`name`, b.`size`, b.`num_files`, b.`is_open` FROM `blocks` AS b'

        if self._mysql.table_exists('datasets_load_tmp'):
            sql += ' INNER JOIN `datasets_load_tmp` AS t ON t.`id` = b.`dataset_id`'

        sql += ' ORDER BY b.`dataset_id`'

        _dataset_id = 0
        dataset = None
        for block_id, dataset_id, name, size, num_files, is_open in self._mysql.xquery(sql):
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id

                dataset = id_dataset_map[dataset_id]
                dataset.blocks.clear()
                dataset.size = 0
                dataset.num_files = 0

                id_block_map = id_block_maps[dataset_id] = {}

            block = Block(
                Block.translate_name(name),
                dataset,
                size,
                num_files,
                is_open
            )

            dataset.blocks.add(block)
            dataset.size += block.size
            dataset.num_files += block.num_files

            id_block_map[block_id] = block

    def _load_replicas(self, inventory, id_group_map, id_site_map, id_dataset_map, id_block_maps):
        sql = 'SELECT dr.`dataset_id`, dr.`site_id`, dr.`is_custodial`,'
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
        for dataset_id, site_id, is_custodial, block_id, group_id, is_complete, b_is_custodial, b_size, b_last_update in self._mysql.xquery(sql):
            if dataset_id != _dataset_id:
                _dataset_id = dataset_id

                dataset = id_dataset_map[_dataset_id]
                dataset.replicas.clear()

                id_block_map = id_block_maps[dataset_id]

            if site_id != _site_id:
                _site_id = site_id
                site = id_site_map[site_id]

            if dataset_replica is None or dataset != dataset_replica.dataset or site != dataset_replica.site:
                if dataset_replica is not None:
                    # add to dataset and site after filling all block replicas
                    # this does not matter for the dataset, but for the site there is some heavy
                    # computation needed when a replica is added
                    dataset.replicas.add(dataset_replica)
                    site.add_dataset_replica(dataset_replica)

                dataset_replica = DatasetReplica(
                    dataset,
                    site,
                    is_custodial = is_custodial
                )

            block = id_block_map[block_id]
            group = id_group_map[group_id]

            block_replica = BlockReplica(
                block,
                site,
                group = group,
                is_complete = is_complete,
                is_custodial = b_is_custodial,
                size = block.size if b_size is None else b_size,
                last_update = b_last_update
            )

            dataset_replica.block_replicas.add(block_replica)

        if dataset_replica is not None:
            # one last bit
            dataset.replicas.add(dataset_replica)
            site.add_dataset_replica(dataset_replica)

    def save_sites(self, sites): #override
        # insert/update sites
        LOG.info('Inserting/updating %d sites.', len(sites))

        fields = ('name', 'host', 'storage_type', 'backend', 'storage', 'cpu', 'status')
        mapping = lambda s: (s.name, s.host, Site.storage_type_name(s.storage_type), s.backend, s.storage, s.cpu, s.status)

        self._mysql.insert_many('sites', fields, mapping, sites)

    def save_groups(self, groups): #override
        # insert/update groups
        LOG.info('Inserting/updating %d groups.', len(groups))

        self._mysql.insert_many('groups', ('name', 'olevel'), lambda g: (g.name, g.olevel.__name__), groups)

    def save_datasets(self, datasets): #override
        # insert/update software versions

        version_map = {None: 0} # tuple -> id
        for vtuple in self._mysql.xquery('SELECT * FROM `software_versions`'):
            version_map[vtuple[1:]] = vtuple[0]

        all_versions = set([d.software_version for d in datasets])
        for v in all_versions:
            if v not in version_map:
                # id = 0 automatically generates the next id
                new_id = self._mysql.query('INSERT INTO `software_versions` VALUES %s' % str((0,) + v))
                version_map[v] = new_id

        # insert/update datasets
        LOG.info('Inserting/updating %d datasets.', len(datasets))

        name_entry_map = {}
        query = 'SELECT `name`, `id`, `size`, `num_files`, `status`+0, `on_tape`, `data_type`+0, `software_version_id`, UNIX_TIMESTAMP(`last_update`), `is_open` FROM `datasets`'
        for entry in self._mysql.xquery(query):
            name_entry_map[entry[0]] = entry[1:]

        datasets_to_update = []
        datasets_to_insert = []

        for dataset in datasets:
            try:
                dataset_id, size, num_files, status, on_tape, data_type, software_version_id, last_update, is_open = name_entry_map.pop(dataset.name)
            except KeyError:
                datasets_to_insert.append(dataset)
                continue

            if dataset.size != size or dataset.num_files != num_files or dataset.status != status or dataset.on_tape != on_tape or \
                    version_map[dataset.software_version] != software_version_id or dataset.last_update != last_update or dataset.is_open != is_open:
                datasets_to_update.append((dataset_id, dataset))

        LOG.info("%d datasets to update", len(datasets_to_update))
        LOG.info("%d datasets to insert", len(datasets_to_insert))

        fields = ('id', 'name', 'size', 'num_files', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda (i, d): (
            i,
            d.name,
            d.size,
            d.num_files,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
            1 if d.is_open else 0
        )

        # use INSERT ON DUPLICATE KEY UPDATE
        self._mysql.insert_many('datasets', fields, mapping, datasets_to_update, do_update = True)

        fields = ('name', 'size', 'num_files', 'status', 'on_tape', 'data_type', 'software_version_id', 'last_update', 'is_open')
        # MySQL expects the local time for last_update
        mapping = lambda d: (
            d.name,
            d.size,
            d.num_files,
            d.status,
            d.on_tape,
            d.data_type,
            version_map[d.software_version],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d.last_update)),
            1 if d.is_open else 0
        )

        self._mysql.insert_many('datasets', fields, mapping, datasets_to_insert, do_update = False)

        # load the ids of the inserted datasets and append to the update list
        dataset_id_map = {}
        self._make_dataset_map(datasets_to_insert, dataset_id_map = dataset_id_map)
        for dataset in datasets_to_insert:
            datasets_to_update.append((dataset_id_map[dataset], dataset))

        # insert/update blocks and files
        LOG.info('Inserting/updating blocks and files.')

        block_ids_to_delete = []
        blocks_to_update = []
        file_ids_to_delete = []
        files_to_update = []
        files_to_insert = []

        # loop over all datasets that were updated or inserted
        for dataset_id, dataset in datasets_to_update:
            blocks_in_mem = dict((b.real_name(), b) for b in dataset.blocks)
            blocks_in_db = self._mysql.xquery('SELECT `id`, `name`, `size`, `num_files`, `is_open` FROM `blocks` WHERE `dataset_id` = %s', dataset_id)
            for block_id, name, size, num_files, is_open in blocks_in_db:
                try:
                    block = blocks_in_mem.pop(name)
                except KeyError:
                    # in DB but not in memory - TODO need to have "invalidated" flag and not delete
                    block_ids_to_delete.append(block_id)
                    continue

                if size != block.size or num_files != block.num_files or is_open != block.is_open:
                    blocks_to_update.append((block_id, block.size, block.num_files, block.is_open))

            # remaining items in blocks_in_mem are all new
            # need to insert here to create block ids
            fields = ('dataset_id', 'name', 'size', 'num_files', 'is_open')
            mapping = lambda b: (dataset_id, b.real_name(), b.size, b.num_files, b.is_open)
            self._mysql.insert_many('blocks', fields, mapping, blocks_in_mem.itervalues(), do_update = False)

            block_id_map = {}
            self._mysql.make_map('blocks', dataset.blocks, object_id_map = block_id_map, key = lambda b: b.real_name())

            for block in dataset.blocks:
                if block.files is None:
                    continue

                block_id = block_id_map[block]

                files_in_mem = dict((f.lfn, f) for f in block.files)
                files_in_db = self._mysql.xquery('SELECT `id`, `name`, `size` FROM `files` WHERE `block_id` = %s', block_id)
                for file_id, name, size in files_in_db:
                    try:
                        lfile = files_in_mem.pop(name)
                    except KeyError:
                        file_ids_to_delete.append(file_id)
                        continue

                    if size != lfile.size:
                        files_to_update.append((file_id, size))

                # remaining items in files_in_mem are all new
                for name, lfile in files_in_mem.iteritems():
                    files_to_insert.append((block_id, dataset_id, lfile.size, name))

        # delete blocks, files, block_replicas, and block_replica_sizes
        sqlbase = 'DELETE b, f, br, brs FROM `blocks` AS b'
        sqlbase += ' LEFT JOIN `files` AS f ON f.`block_id` = b.`id`'
        sqlbase += ' LEFT JOIN `block_replicas` AS br ON br.`block_id` = b.`id`'
        sqlbase += ' LEFT JOIN `block_replica_sizes` AS brs ON brs.`block_id` = b.`id`'

        self._mysql.execute_many(sqlbase, 'b.`id`', block_ids_to_delete)

        # delete files
        self._mysql.delete_many('files', 'id', file_ids_to_delete)

        # update blocks
        fields = ('id', 'size', 'num_files', 'is_open')
        # use INSERT ON DUPLICATE KEY UPDATE query
        self._mysql.insert_many('blocks', fields, None, blocks_to_update, do_update = True)

        # update files
        fields = ('id', 'size')
        self._mysql.insert_many('files', fields, None, files_to_update, do_update = True)

        # insert files
        fields = ('block_id', 'dataset_id', 'size', 'name')
        self._mysql.insert_many('files', fields, None, files_to_insert, do_update = False)

    def update_replicas(self, sites, groups, datasets): #override
        site_id_map = {}
        self._make_site_map(sites, site_id_map = site_id_map)
        group_id_map = {}
        self._make_group_map(groups, group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        # insert/update dataset replicas
        LOG.info('Updating replicas.')

        fields = ('dataset_id', 'site_id', 'is_custodial')
        mapping = lambda r: (dataset_id_map[r.dataset], site_id_map[r.site], r.is_custodial)

        all_replicas = []
        for dataset in datasets:
            if dataset.replicas is not None:
                all_replicas.extend(dataset.replicas)

        self._mysql.insert_many('dataset_replicas', fields, mapping, all_replicas, do_update = True)

        # delete dataset replicas + insert/update/delete block replicas

        all_replicas = []
        replica_sizes = []
        for dataset in datasets:
            # delta deletions: load here information of all dataset replicas/block replicas from inventory (inventory.store.load_dataset).
            inventory_dataset = self.load_dataset(dataset.name, load_blocks = True, load_files = False, load_replicas = True, sites = sites, groups = groups)

            for replica in dataset.replicas:
                site_id = site_id_map[replica.site]
                blocks_in_memory = set(r.block for r in replica.block_replicas)

                inventory_dataset_replica = inventory_dataset.find_replica(replica.site)

                block_replicas_to_delete = []
                for inventory_block_replica in inventory_dataset_replica.block_replicas:
                    if inventory_block_replica.block not in blocks_in_memory:
                        block_replicas_to_delete.append(inventory_block_replica)

                self.delete_blockreplicas(block_replicas_to_delete)

            # delete dataset replicas that are in DB but not in memory
            for inventory_replica in inventory_dataset.replicas:
                site = inventory_replica.site
                if site in sites and dataset.find_replica(site) is None:
                    self.delete_datasetreplica(inventory_replica, delete_blockreplicas = True)

            # end of delta deletions part
            # remaining block replicas are to be inserted

            block_name_to_id = {}
            for block_id, block_name in self._mysql.xquery('SELECT `id`, `name` FROM `blocks` WHERE `dataset_id` = %s', dataset_id_map[dataset]):
                block_name_to_id[Block.translate_name(block_name)] = block_id

            for replica in dataset.replicas:
                site_id = site_id_map[replica.site]

                for block_replica in replica.block_replicas:
                    block_id = block_name_to_id[block_replica.block.name]
                    last_update_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block_replica.last_update))

                    all_replicas.append((block_id, site_id, group_id_map[block_replica.group], block_replica.is_complete, block_replica.is_custodial, last_update_timestamp))
                    if not block_replica.is_complete:
                        replica_sizes.append((block_id, site_id, block_replica.size))

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial', 'last_update')
        self._mysql.insert_many('block_replicas', fields, None, all_replicas, do_update = True)

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes', fields, None, replica_sizes, do_update = True)

    def save_replicas(self, sites, groups, datasets): #override
        if len(sites) == 0:
            # we have no sites loaded in memory -> cannot have replicas -> nothing to do
            return

        site_id_map = {}
        self._make_site_map(sites, site_id_map = site_id_map)
        group_id_map = {}
        self._make_group_map(groups, group_id_map = group_id_map)
        dataset_id_map = {}
        self._make_dataset_map(datasets, dataset_id_map = dataset_id_map)

        site_id_list = '(' + ','.join('%d' % sid for sid in site_id_map.values()) + ')'

        # insert/update dataset replicas
        LOG.info('Inserting/updating dataset replicas.')

        self._mysql.query('DELETE FROM `dataset_replicas` WHERE `site_id` IN ' + site_id_list)

        fields = ('dataset_id', 'site_id', 'is_custodial')
        mapping = lambda r: (dataset_id_map[r.dataset], site_id_map[r.site], r.is_custodial)

        all_replicas = []
        for dataset in datasets:
            if dataset.replicas is not None:
                all_replicas.extend(dataset.replicas)

        self._mysql.insert_many('dataset_replicas', fields, mapping, all_replicas, do_update = False)

        # insert/update block replicas
        LOG.info('Inserting/updating block replicas.')

        all_replicas = []
        replica_sizes = []
        for dataset in datasets:
            block_name_to_id = {}
            for block_id, block_name in self._mysql.xquery('SELECT `id`, `name` FROM `blocks` WHERE `dataset_id` = %s', dataset_id_map[dataset]):
                block_name_to_id[Block.translate_name(block_name)] = block_id

            for replica in dataset.replicas:
                site_id = site_id_map[replica.site]
                for block_replica in replica.block_replicas:
                    block_id = block_name_to_id[block_replica.block.name]

                    last_update_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(block_replica.last_update))
                    all_replicas.append((block_id, site_id, group_id_map[block_replica.group], block_replica.is_complete, block_replica.is_custodial, last_update_timestamp))
                    if not block_replica.is_complete:
                        replica_sizes.append((block_id, site_id, block_replica.size))

        self._mysql.query('DELETE FROM `block_replicas` WHERE `site_id` IN ' + site_id_list)

        fields = ('block_id', 'site_id', 'group_id', 'is_complete', 'is_custodial', 'last_update')
        self._mysql.insert_many('block_replicas', fields, None, all_replicas, do_update = False)

        self._mysql.query('DELETE FROM `block_replica_sizes` WHERE `site_id` IN ' + site_id_list)

        fields = ('block_id', 'site_id', 'size')
        self._mysql.insert_many('block_replica_sizes', fields, None, replica_sizes, do_update = False)

    def _make_site_map(self, sites, site_id_map = None, id_site_map = None):
        self._mysql.make_map('sites', sites, site_id_map, id_site_map)

    def _make_group_map(self, groups, group_id_map = None, id_group_map = None):
        # Sometimes when calling do_update_blockreplicas it can be we're handing over group 'None'
        cleansed_groups = [g for g in groups if g is not None]

        if len(cleansed_groups) > 0:
            self._mysql.make_map('groups', cleansed_groups, group_id_map, id_group_map)
        if group_id_map is not None:
            group_id_map[None] = 0
        if id_group_map is not None:
            id_group_map[0] = None

    def _make_dataset_map(self, datasets, dataset_id_map = None, id_dataset_map = None):
        try:
            tmp_join = (len(datasets) < 1000)
        except TypeError:
            tmp_join = False

        self._mysql.make_map('datasets', datasets, dataset_id_map, id_dataset_map, tmp_join = tmp_join)

