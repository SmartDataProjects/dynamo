import logging
import time
import re
import collections
import fnmatch
import threading

from source.groupinfo import GroupInfoSourceInterface
from source.siteinfo import SiteInfoSourceInterface
from source.datasetinfo import DatasetInfoSourceInterface
from source.replicainfo import ReplicaInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from dataformat import Dataset, Block, File, Site, Group, DatasetReplica, BlockReplica
from common.thread import parallel_exec
import common.configuration as config

LOG = logging.getLogger(__name__)

# Using POST requests with PhEDEx:
# Accumulate dataset=/A/B/C options and make a query once every 10000 entries
# PhEDEx does not document a hard limit on the length of POST request list.
# 10000 was experimentally verified to be OK.

class PhEDExDBSSSB(GroupInfoSourceInterface, SiteInfoSourceInterface, DatasetInfoSourceInterface, ReplicaInfoSourceInterface):
    """
    Interface to PhEDEx/DBS/SSB using datasvc REST API.
    """

    def __init__(self, config):
        GroupInfoSourceInterface.__init__(self, config)
        SiteInfoSourceInterface.__init__(self, config)
        DatasetInfoSourceInterface.__init__(self, config)
        ReplicaInfoSourceInterface.__init__(self, config)

        self._phedex_interface = RESTService(config.phedex_url, use_cache = True)
        self._dbs_interface = RESTService(config.dbs_url) # needed for detailed dataset info
        self._ssb_interface = RESTService(config.ssb_url) # needed for site status

    def get_site_list(self): #override (SiteInfoSourceInterface)
        options = []

        if include is not None:
            options.extend('node=%s' % s for s in include)

        LOG.info('get_site_list  Fetching the list of nodes from PhEDEx')

        site_list = []

        for entry in self._make_phedex_request('nodes', options):
            site_list.append(Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology']))

        return site_list
        
    def set_site_status(self, sites): #override (SiteInfoSourceInterface)
        for site in sites.itervalues():
            site.status = Site.STAT_READY

        # get list of sites in waiting room (153) and morgue (199)
        for colid, stat in [(153, Site.STAT_WAITROOM), (199, Site.STAT_MORGUE)]:
            result = self._ssb_interface.make_request('getplotdata', 'columnid=%d&time=2184&dateFrom=&dateTo=&sites=all&clouds=undefined&batch=1' % colid)
            try:
                source = result['csvdata']
            except KeyError:
                logger.error('SSB parse error')
                return

            latest_timestamp = {}
    
            for entry in source:
                try:
                    site = sites[entry['VOName']]
                except KeyError:
                    continue
                
                # entry['Time'] is UTC but we are only interested in relative times here
                timestamp = time.mktime(time.strptime(entry['Time'], '%Y-%m-%dT%H:%M:%S'))
                if site in latest_timestamp and latest_timestamp[site] > timestamp:
                    continue

                latest_timestamp[site] = timestamp

                if entry['Status'] == 'in':
                    site.status = stat
                else:
                    site.status = Site.STAT_READY

    def get_group_list(self, groups, filt = '*'): #override (GroupInfoSourceInterface)
        logger.info('get_group_list  Fetching the list of groups from PhEDEx')
        source = self._make_phedex_request('groups')

        if type(filt) is str:
            filt = [filt]
        
        for entry in source:
            name = entry['name']

            if name in groups:
                continue

            for f in filt:
                if fnmatch.fnmatch(name, f):
                    break
            else:
                continue

            group = Group(name)
            groups[name] = group
    
    def make_replica_links(self, inventory, site_filt = '*', group_filt = '*', dataset_filt = '*', last_update = 0): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site (or a list corresponding to new replicas created 
        since the last inventory update).
        Objects in sites and datasets should have replica information cleared. All block replica objects
        are newly created within this function.
        Implementation:
        1. Call PhEDEx blockreplicas with show_dataset=y to obtain a JSON structure of [{dataset: [{block: [replica]}]}]
        2. Loop over JSON:
          2.1 Unknown dataset -> create object
          2.2 Loop over blocks:
            2.2.1 Unknown block -> create object
            2.2.2 Block updated -> set dataset.status to PRODUCTION and update block
            2.2.3 Loop over block replicas:
              2.2.3.1 If the replica is not owned by any of the specified groups, skip
              2.2.3.2 Unknown dataset replica -> create object (can be known in case of parallel execution)
              2.2.3.3 Create block replica object
        3. For each dataset, make a list of blocks with replicas and compare it with the list of blocks.
           If they differ, some blocks may be invalidated. Set dataset.status to PRODUCTION. (Triggers detailed
           inquiry in set_dataset_details)
        4. Remove datasets with no replicas from memory (simple speed optimization)

        @param inventory    InventoryManager instance
        @param site_filt    Limit to replicas on sites matching the pattern.
        @param group_filt   Limit to replicas owned by groups matching the pattern.
        @param dataset_filt Limit to replicas of datasets matching the pattern.
        """

        if last_update > 0:
            # query URL will be different every time - need to turn caching off
            cache_lifetime = config.phedex.cache_lifetime
            config.phedex.cache_lifetime = 0

        counters = {
            'new_datasets': 0,
            'datasets_with_new_blocks': 0,
            'datasets_with_updated_blocks': 0,
            'datasets_with_updated_blocklist': 0,
            'new_blocks': 0
        }

        all_sites = [site for name, site in inventory.sites.iteritems() if fnmatch.fnmatch(name, site_filt)]
        all_groups = [group for name, group in inventory.groups.iteritems() if fnmatch.fnmatch(name, group_filt)]

        if dataset_filt == '*':
            # PhEDEx only accepts form /*/*/*
            dataset_filt = '/*/*/*'

        if dataset_filt == '/*/*/*' and last_update == 0:
            items = []
            for site in all_sites:
                total_quota = site.quota()
                if total_quota < 0 or total_quota >= 500:
                    # negative quota -> "infinite"
                    # For large sites, further split by the first character of the dataset names
                    # Splitting depends on the quota
                    characters = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
                    if total_quota > 0:
                        chunk_size = max(len(characters) / int(total_quota / 100), 1)
                    else:
                        chunk_size = 1

                    charsets = [characters[i:i + chunk_size] for i in range(0, len(characters), chunk_size)]
                    for charset in charsets:
                        items.append((inventory, [site], all_groups, ['/%s*/*/*' % c for c in charset], last_update, counters))
                else:
                    items.append((inventory, [site], all_groups, ['/*/*/*'], last_update, counters))

            logger.info('make_replica_links  Fetching block replica information from PhEDEx')
            parallel_exec(self._check_blockreplicas, items, num_threads = min(32, len(items)), print_progress = True, timeout = 7200)
            logger.info('make_replica_links  Fetching subscription information from PhEDEx')
            parallel_exec(self._check_subscriptions, items, num_threads = min(32, len(items)), print_progress = True, timeout = 7200)
            del items
        elif dataset_filt != '/*/*/*' and last_update == 0:
            logger.info('make_replica_links  Fetching block replica information from PhEDEx')
            self._check_blockreplicas(inventory, all_sites, all_groups, [dataset_filt], last_update, counters)
            logger.info('make_replica_links  Fetching subscription information from PhEDEx')
            self._check_subscriptions(inventory, all_sites, all_groups, [dataset_filt], last_update, counters)

        if last_update > 0:
            # delta part - can go serial (in fact HAS TO!)
            logger.info('make_replica_links  Fetching block replica information from PhEDEx')
            self._check_blockreplicas(inventory, all_sites, all_groups, [dataset_filt], last_update, counters)
            logger.info('make_replica_links  Fetching subscription information from PhEDEx')
            self._check_subscriptions(inventory, all_sites, all_groups, [dataset_filt], last_update, counters)
            logger.info('make_replica_links  Fetching deletion information from PhEDEx')
            self._check_deletions(inventory, all_sites, all_groups, dataset_filt, last_update)

        # Following dataset status check only works for full updates!! Need to come up with a way to do this in delta
        if last_update == 0:
            logger.info('Checking dataset status changes.')

            if dataset_filt == '/*/*/*':    
                invalid_or_deprecated_or_deleted = set(d for d in inventory.datasets.itervalues() if d.status in (Dataset.STAT_INVALID, Dataset.STAT_DEPRECATED, Dataset.STAT_DELETED))
            else:
                invalid_or_deprecated_or_deleted = set(d for d in inventory.datasets.itervalues() if d.status in (Dataset.STAT_INVALID, Dataset.STAT_DEPRECATED, Dataset.STAT_DELETED) and fnmatch.fnmatch(d.name, dataset_filt))

            def confirm_status(status, status_bit):
                options = ['dataset_access_type=' + status]
                if dataset_filt != '/*/*/*':    
                    options += ['dataset=' + dataset_filt]

                dbs_entries = self._make_dbs_request('datasets', options)
                for ds_entry in dbs_entries:
                    try:
                        dataset = inventory.datasets[ds_entry['dataset']]
                    except KeyError:
                        continue
        
                    dataset.status = status_bit
        
                    try:
                        invalid_or_deprecated_or_deleted.remove(dataset)
                    except KeyError:
                        pass

            confirm_status('INVALID', Dataset.STAT_INVALID)
            confirm_status('DEPRECATED', Dataset.STAT_DEPRECATED)
            confirm_status('DELETED', Dataset.STAT_DELETED)
    
            # remaining datasets in the list must have been revalidated
            # set it to production to trigger further inspection
            for dataset in invalid_or_deprecated_or_deleted:
                logger.info('%s was invalid, deprecated, or deleted but not any more', dataset.name)
                dataset.status = Dataset.STAT_PRODUCTION

        logger.info('Checking for updated datasets.')

        for dataset in inventory.datasets.itervalues():
            if dataset.status != Dataset.STAT_VALID:
                # dataset is already marked for further inspection or ignored
                continue

            # check for potentially invalidated blocks
            blocks_with_replicas = set()
            for replica in dataset.replicas:
                blocks_with_replicas.update([r.block for r in replica.block_replicas])

            if blocks_with_replicas != set(dataset.blocks):
                counters['datasets_with_updated_blocklist'] += 1
                dataset.status = Dataset.STAT_PRODUCTION # trigger DBS query

        if last_update > 0:
            # restore caching
            config.phedex.cache_lifetime = cache_lifetime

        logger.info('Done.')
        logger.info(' %d new datasets', counters['new_datasets'])
        logger.info(' %d new blocks', counters['new_blocks'])
        logger.info(' %d datasets with new blocks', counters['datasets_with_new_blocks'])
        logger.info(' %d datasets with updated blocks', counters['datasets_with_updated_blocks'])
        logger.info(' %d datasets with updated blocklist', counters['datasets_with_updated_blocklist'])

    def _check_blockreplicas(self, inventory, site_list, group_list, dname_list, last_update, counters):
        if len(site_list) == 1:
            logger.debug('Fetching replica info on %s.', site_list[0].name)

        gname_list = [g.name for g in group_list] + [None]

        options = ['show_dataset=y']

        if last_update > 0:
            options.append('update_since=%d' % last_update)

        for site in site_list:
            options.append('node=' + site.name)

        for dname in dname_list:
            options.append('dataset=' + dname)

        source = self._make_phedex_request('blockreplicas', options)

        # process retrieved data under a lock - otherwise can cause inconsistencies when e.g. block info is updated between one phedex call and another.
        with lock:
            for dataset_entry in source:
                if 'block' not in dataset_entry:
                    continue
                
                ds_name = dataset_entry['name']

                new_dataset = False

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    dataset, in_store = inventory.load_dataset(ds_name, load_blocks = True, load_files = False, load_replicas = (last_update > 0), sites = site_list, groups = group_list)

                    if not in_store:
                        new_dataset = True
                        counters['new_datasets'] += 1

                dataset.is_open = (dataset_entry['is_open'] == 'y')
                dataset.replicas.clear()

                dataset_replica = None

                new_block = False
                updated_block = False

                for block_entry in dataset_entry['block']:
                    logger.debug('Block %s', block_entry['name'])

                    try:
                        block_name = Block.translate_name(block_entry['name'].replace(ds_name + '#', ''))
                    except:
                        logger.error('Invalid block name %s in blockreplicas', ds_name)
                        continue

                    block = None
                    if not new_dataset:
                        block = dataset.find_block(block_name)

                    if block is None:
                        logger.debug('Creating new block %s', block_entry['name'])

                        block = Block(
                            block_name,
                            dataset = dataset,
                            size = block_entry['bytes'],
                            num_files = block_entry['files'],
                            is_open = (block_entry['is_open'] == 'y')
                        )

                        dataset.blocks.add(block)
                        dataset.size += block.size
                        dataset.num_files += block.num_files
                        if dataset.status == Dataset.STAT_VALID:
                            # there are some pretty crazy cases with ignored datasets. We've seen cases like two datasets with identical name, each
                            # with its own list of blocks where PhEDEx "blockreplicas" and "data" and DBS "blocks" all don't agree
                            dataset.status = Dataset.STAT_PRODUCTION # trigger DBS query
                        
                            counters['new_blocks'] += 1
                            new_block = True

                    elif block.size != block_entry['bytes'] or \
                            block.num_files != block_entry['files'] or \
                            block.is_open != (block_entry['is_open'] == 'y'):
                        # block record was updated
                        logger.debug('Block %s record was updated', block.real_name())

                        block.size = block_entry['bytes']
                        block.num_files = block_entry['files']
                        block.is_open = (block_entry['is_open'] == 'y')

                        if dataset.status == Dataset.STAT_VALID:
                            dataset.status = Dataset.STAT_PRODUCTION

                            updated_block = True

                    for replica_entry in block_entry['replica']:
                        if replica_entry['group'] not in gname_list:
                            continue

                        if replica_entry['group'] is not None:
                            try:
                                group = inventory.groups[replica_entry['group']]
                            except KeyError:
                                logger.warning('Group %s for replica of block %s not registered.', replica_entry['group'], block.real_name())
                                group = None
                        else:
                            group = None

                        site = inventory.sites[replica_entry['node']]

                        if dataset_replica is None or dataset_replica.site != site:
                            logger.debug('New site %s', site.name)
                            dataset_replica = dataset.find_replica(site)

                            if dataset_replica is None:
                                dataset_replica = DatasetReplica(dataset, site)
                                dataset.replicas.add(dataset_replica)
                                site.add_dataset_replica(dataset_replica)

                                # first time associating this dataset with this site
                                logger.debug('Instantiating dataset replica at %s', site.name)
    
                                # start with is_complete = True, update if any block replica is incomplete
                                dataset_replica.is_complete = True

                                if site.storage_type == Site.TYPE_MSS:
                                    # start with partial - update to full if the dataset replica is indeed full
                                    dataset.on_tape = Dataset.TAPE_PARTIAL

                        if int(replica_entry['time_update']) > dataset_replica.last_block_created:
                            dataset_replica.last_block_created = int(replica_entry['time_update'])

                        # PhEDEx 'complete' flag cannot be trusted; defining completeness in terms of size.
                        is_complete = (replica_entry['bytes'] == block.size)
                        is_custodial = (replica_entry['custodial'] == 'y')

                        # if any block replica is not complete, dataset replica is not
                        if not is_complete:
                            dataset_replica.is_complete = False

                        # if any of the block replica is custodial, dataset replica also is
                        if is_custodial:
                            dataset_replica.is_custodial = True

                        block_replica = dataset_replica.find_block_replica(block)
                        
                        if block_replica is None:
                            # if not from_delta or simply a new block replica
                            logger.debug('New BlockReplica of %s', block.real_name())

                            block_replica = BlockReplica(
                                block,
                                site,
                                group,
                                is_complete, 
                                is_custodial,
                                size = replica_entry['bytes'],
                                last_update = int(replica_entry['time_update'])
                            )

                            dataset_replica.block_replicas.add(block_replica)
                            site.add_block_replica(block_replica)

                        else:
                            block_replica.group = group
                            block_replica.is_complete = is_complete
                            block_replica.is_custodial = is_custodial
                            block_replica.size = replica_entry['bytes']
                            block_replica.last_update = int(replica_entry['time_update'])

                        if site.storage_type == Site.TYPE_MSS:
                            # ask whether the dataset replica is full after encountering each block
                            if dataset_replica.is_full():
                                dataset.on_tape = Dataset.TAPE_FULL

                if new_block:
                    counters['datasets_with_new_blocks'] += 1
                if updated_block:
                    counters['datasets_with_updated_blocks'] += 1

        # closes with lock:

    def _check_subscriptions(self, inventory, site_list, group_list, dname_list, last_update, counters):
        # Blockreplicas should give all information about dataset and block replicas that
        # have at least a byte physically copied at the site. We collect data on empty
        # dataset and block replicas (scheduled to be at the site but has no blocks yet)
        # using the subscriptions command.

        gname_list = [g.name for g in group_list] + [None]

        options = ['percent_max=0']
        
        if last_update > 0:
            options.append('create_since=%d' % last_update)

        for site in site_list:
            options.append('node=' + site.name)

        for dname in dname_list:
            options.append('dataset=' + dname)
            # we will only query for dataset-level subscriptions
            # missing empty block-level subscriptions are marginal accounting errors, and block= query is VERY slow.
            # options.append('block=' + dname + '%23*')

        source = self._make_phedex_request('subscriptions', options)

        with lock:
            for dataset_entry in source:
                ds_name = dataset_entry['name']

                new_dataset = False

                try:
                    dataset = inventory.datasets[ds_name]
                except KeyError:
                    dataset, in_store = inventory.load_dataset(ds_name, load_blocks = True, load_files = False, load_replicas = (last_update > 0), sites = site_list, groups = group_list)

                    if not in_store:
                        new_dataset = True
                        counters['new_datasets'] += 1

                    dataset.replicas.clear()

                if 'subscription' in dataset_entry:
                    for subscription in dataset_entry['subscription']:
                        if subscription['node_bytes'] != 0:
                            # We are only looking for empty subscriptions
                            continue

                        if subscription['group'] not in gname_list:
                            continue
                        
                        site = inventory.sites[subscription['node']]

                        dataset_replica = dataset.find_replica(site)

                        if dataset_replica is None:
                            dataset_replica = DatasetReplica(dataset, site)
                            dataset.replicas.add(dataset_replica)
                            site.add_dataset_replica(dataset_replica)

                        dataset_replica.is_custodial = (subscription['custodial'] == 'y')

                        if site.storage_type == Site.TYPE_MSS:
                            if dataset_replica.is_complete:
                                dataset.on_tape = Dataset.TAPE_FULL
                            elif dataset.on_tape != Dataset.TAPE_FULL:
                                dataset.on_tape = Dataset.TAPE_PARTIAL

                if 'block' in dataset_entry:
                    for block_entry in dataset_entry['block']:
                        if 'subscription' not in block_entry:
                            continue

                        try:
                            block_name = Block.translate_name(block_entry['name'].replace(ds_name + '#', ''))
                        except:
                            logger.error('Invalid block name %s in blockreplicas', ds_name)
                            continue

                        block = dataset.find_block(block_name)

                        for subscription in block_entry['subscription']:
                            if subscription['node_bytes'] != 0:
                                # We are only looking for empty subscriptions
                                continue

                            if subscription['group'] not in gname_list:
                                continue
                            
                            site = inventory.sites[subscription['node']]

                            if subscription['group'] is not None:
                                try:
                                    group = inventory.groups[subscription['group']]
                                except KeyError:
                                    group = None
                            else:
                                group = None

                            dataset_replica = dataset.find_replica(site)
                            
                            if dataset_replica is None:
                                dataset_replica = DatasetReplica(dataset, site)
                                dataset.replicas.add(dataset_replica)
                                site.add_dataset_replica(dataset_replica)

                            is_custodial = (subscription['custodial'] == 'y')
                            dataset_replica.is_custodial = is_custodial

                            block_replica = dataset_replica.find_block_replica(block)

                            if block_replica is None:
                                block_replica = BlockReplica(
                                    block,
                                    site,
                                    group,
                                    False,
                                    is_custodial,
                                    size = 0,
                                    last_update = 0
                                )

                                dataset_replica.block_replicas.add(block_replica)
                                site.add_block_replica(block_replica)

                            else:
                                block_replica.group = group
                                block_replica.is_complete = False
                                block_replica.is_custodial = is_custodial
                                block_replica.size = 0
                                block_replica.last_update = 0

    def _check_deletions(self, inventory, site_list, group_list, dataset_filt, last_update):
        logger.info('Checking for deleted dataset and block replicas.')

        for site in site_list:
            options = ['node=%s' % site.name, 'complete=y', 'complete_since=%d' % last_update]
            if dataset_filt != '/*/*/*':
                options += ['dataset=' + dataset_filt]

            deletions = self._make_phedex_request('deletions', options)

            for phedex_entry in deletions:
                ds_name = phedex_entry['name']
                try:
                    dataset = inventory.datasets[ds_name]
                    logger.debug("Found dataset %s in memory", ds_name)
                except KeyError:
                    logger.debug("Loading dataset %s", ds_name)
                    dataset, in_store = inventory.load_dataset(ds_name, load_blocks = True, load_files = False, load_replicas = True, sites = site_list, groups = group_list)
                    
                dataset_replica = dataset.find_replica(site)

                if dataset_replica is None:
                    logger.error('Trying to delete blocks from dataset_replica of %s that does not exist on site %s.' % (ds_name, site))
                    continue

                for block_entry in phedex_entry['block']:
                    block_name = block_entry['name'].split('#', 1)[1]
                    block_replica = dataset_replica.find_block_replica(Block.translate_name(block_name))
                    if block_replica is None:
                        logger.error('Trying to delete a block %s that is not in the dataset replica.' % block_name)
                        continue
                    else:
                        dataset_replica.block_replicas.remove(block_replica)

                if len(dataset_replica.block_replicas) == 0:
                    dataset.replicas.remove(dataset_replica)
                    site.remove_dataset_replica(dataset_replica)
                else:
                    site.update_partitioning(dataset_replica)

                if site.storage_type == Site.TYPE_MSS:
                    # A (part of) tape replica was deleted. Update the on_tape flag of the dataset.
                    dataset.on_tape = Dataset.TAPE_NONE
                    for replica in dataset.replicas:
                        if replica.site.storage_type == Site.TYPE_MSS:
                            if replica.is_full():
                                dataset.on_tape = Dataset.TAPE_FULL
                            elif dataset.on_tape != Dataset.TAPE_FULL:
                                dataset.on_tape = Dataset.TAPE_PARTIAL

    def find_tape_copies(self, inventory): #override (ReplicaInfoSourceInterface)
        """
        Use 'subscriptions' query to check if all blocks of the dataset are on tape.
        Queries only for datasets where on_tape != FULL and status in [VALID, PRODUCTION].
        site=T*MSS -> tape
        """

        # Routine to fetch data and fill the list of blocks on tape
        def inquire_phedex(dataset_list):
            options = [('create_since', 0), ('node', 'T*_MSS')]
            options.extend([('block', dataset.name + '#*') for dataset in dataset_list]) # this will fetch dataset-level subscriptions too

            source = self._make_phedex_request('subscriptions', options, method = POST)
           
            # Elements of the returned list has a structure
            # {'name': dataset_name, 'bytes': N, .., 'subscription': [ds_subscriptions], 'block': [blocks]}
            # ds_subscription:
            # {'custodial': 'y/n', .., 'node_bytes': N}
            # block:
            # {'bytes': N, 'name': DS#BL, 'subscription': [bl_subscriptions]}
            # bl_subscription:
            # {'custodial': 'y/n', .., 'node_bytes': N}
            #
            # dataset.on_tape = TAPE_FULL if
            #  . A ds_subscription with node_bytes = bytes exist, or
            #  . All blocks are at one node and node_bytes = bytes
            # dataset.on_tape = TAPE_PARTIAL if not TAPE_FULL and
            #  . A ds_subscription exists, or
            #  . A bl_subscription exists
            # dataset.on_tape = TAPE_NONE if not (TAPE_FULL or TAPE_PARTIAL)
 
            for ds_entry in source:
                dataset = inventory.datasets[ds_entry['name']]

                ds_bytes = ds_entry['bytes']
 
                # if a dataset-level or block-level subscription exists, it's at least partial
 
                if 'subscription' in ds_entry:
                    if len(ds_entry['subscription']) != 0:
                        dataset.on_tape = Dataset.TAPE_PARTIAL
 
                    for ds_subscription in ds_entry['subscription']:
                        if ds_subscription['node_bytes'] == ds_bytes:
                            dataset.on_tape = Dataset.TAPE_FULL
                            break
     
                    if dataset.on_tape == Dataset.TAPE_FULL:
                        # no more need to process data
                        continue
  
                if 'block' in ds_entry:
                    if len(ds_entry['block']) != 0:
                        dataset.on_tape = Dataset.TAPE_PARTIAL
  
                    block_names = set(b.name for b in dataset.blocks)
                    # collect the list of blocks subscribed at each tape site
                    blocks_at_sites = collections.defaultdict(set)
     
                    for bl_entry in ds_entry['block']:
                        name = bl_entry['name']
  
                        try:
                            block_name = Block.translate_name(name[name.find('#') + 1:])
                        except:
                            logger.error('Invalid block name %s in subscriptions', name)
                            continue
     
                        bl_bytes = bl_entry['bytes']
     
                        for bl_subscription in bl_entry['subscription']:
                            # only consider full block replicas
                            if bl_subscription['node_bytes'] == bl_bytes:
                                blocks_at_sites[bl_subscription['node']].add(block_name)

                    # if there is at least one tape site with a full list of block replicas, set on_tape to TAPE_FULL and be done
                    for names in blocks_at_sites.itervalues():
                        if names == block_names:
                            dataset.on_tape = Dataset.TAPE_FULL
                            break


        # this function called only during full update - we have all datasets we care about in memory already
        chunk_size = 1000
        dataset_chunks = [[]]

        # Loop over datasets not on tape
        for dataset in inventory.datasets.itervalues():
           # on_tape is TAPE_NONE by default
           if dataset.on_tape == Dataset.TAPE_FULL:
               continue

           if dataset.status != Dataset.STAT_VALID and dataset.status != Dataset.STAT_PRODUCTION:
               continue

           # set it back to NONE first
           dataset.on_tape = Dataset.TAPE_NONE

           dataset_chunks[-1].append(dataset)
           if len(dataset_chunks[-1]) == chunk_size:
               dataset_chunks.append([])

        if len(dataset_chunks[-1]) == 0:
            dataset_chunks.pop()
        
        ntotal = sum(len(c) for c in dataset_chunks)
        logger.info('find_tape_copies  Checking tape copies of %d datasets.', ntotal)
        
        parallel_exec(inquire_phedex, dataset_chunks, print_progress = (ntotal > 1000))

    def replica_exists_at_site(self, site, item): #override (ReplicaInfoSourceInterface)
        """
        Argument item can be a Dataset, Block, or File. Returns true if a replica exists at the site.
        """

        options = ['node=' + site.name]

        if type(item) == Dataset:
            options += ['dataset=' + item.name, 'show_dataset=y']
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'show_dataset=y']
        elif type(item) == Block:
            options += ['block=' + item.dataset.name + '%23' + item.real_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.dataset.name + '%23' + item.block.real_name()]
        else:
            raise RuntimeError('Invalid input passed: ' + repr(item))
        
        source = self._make_phedex_request('blockreplicas', options)

        return len(source) != 0

    def set_dataset_details(self, datasets): #override (DatasetInfoSourceInterface)
        """
        Argument datasets is a {name: dataset} dict.
        skip_valid is True for routine inventory update.

        @param datasets  List of datasets to be updated
        """

        logger.info('set_dataset_details  Finding blocks and files for %d datasets.', len(datasets))

        self._set_dataset_constituent_info(datasets)

        logger.info('set_dataset_details  Setting status of %d datasets.', len(datasets))

        # DBS 'datasetlist' query. Sets not just the status but also the dataset type.
        self._set_dataset_status_and_type(datasets)

        # some datasets may become IGNORED after set_dataset_status_and_type
        release_unknown = filter(lambda d: d.status != Dataset.STAT_IGNORED and d.software_version is None, datasets)

        logger.info('set_dataset_details  Finding the software version for %d datasets.', len(release_unknown))

        self._set_software_version_info(release_unknown)

    def _set_dataset_constituent_info(self, datasets):
        """
        Query phedex "data" interface and fill the list of blocks.
        Argument is a list of datasets.
        """

        def inquire_phedex(list_chunk):
            # need to combine the results of two queries (level=block and level=file)
            # because level=file skips datasets and blocks with 0 files

            result = dict()

            # PhEDEx sometimes fails to return data of all datasets. The behavior is reproducible but not predictable to me..
            # We therefore retry queries until all datasets are covered, or all single-dataset queries return nothing.
            dataset_names = [d.name for d in list_chunk]
            
            while len(dataset_names) != 0:
                options = [('level', 'block')]
                options.extend([('dataset', n) for n in dataset_names])
                response = self._make_phedex_request('data', options, method = POST)

                if len(response) == 0:
                    response = [{'dataset': []}]
                    # go one by one
                    for n in dataset_names:
                        options = [('level', 'block'), ('dataset', n)]
                        resp = self._make_phedex_request('data', options, method = POST)

                        if len(resp) == 0:
                            dataset_names.remove(n)
                        else:
                            response[0]['dataset'].append(resp[0]['dataset'][0])

                for entry in response[0]['dataset']:
                    dataset_names.remove(entry['name'])

                    # as crazy as it sounds, PhEDEx can have multiple independent records of identically named datasets
                    try:
                        result[entry['name']]['block'].extend(entry['block'])
                    except KeyError:
                        result[entry['name']] = entry

            # Repeat with level=file
            dataset_names = [d.name for d in list_chunk]

            while len(dataset_names) != 0:
                options = [('level', 'file')]
                options.extend([('dataset', n) for n in dataset_names])
                response = self._make_phedex_request('data', options, method = POST)

                if len(response) == 0:
                    response = [{'dataset': []}]
                    # go one by one
                    for n in dataset_names:
                        options = [('level', 'file'), ('dataset', n)]
                        resp = self._make_phedex_request('data', options, method = POST)
                        if len(resp) == 0:
                            dataset_names.remove(n)
                        else:
                            response[0]['dataset'].append(resp[0]['dataset'][0])

                for ds_update_entry in response[0]['dataset']:
                    dataset_names.remove(ds_update_entry['name'])

                    files_dict = dict((b['name'], b['file']) for b in ds_update_entry['block'])
                    for block_entry in result[ds_update_entry['name']]['block']:
                        try:
                            file_entries = files_dict[block_entry['name']]
                        except KeyError:
                            continue
    
                        block_entry['file'] = file_entries

            # parts of this block that works solely on individual datasets (as opposed to making changes to sites) don't need to be locked
            # but this is a pure processing code (no I/O) and therefore locking doesn't matter as long as there is python Global Interpreter Lock
            with lock:
                for dataset in list_chunk:
                    try:
                        ds_entry = result[dataset.name]
                    except KeyError:
                        # This function is called after make_replica_links
                        # i.e. "blockreplicas" knows about this dataset but "data" doesn't.
                        # i.e. something is screwed up.
                        # We used to set the status to IGNORED, but this would cause problems
                        # with very new datasets.
                        dataset.is_open = True
                        dataset.status = Dataset.STAT_UNKNOWN
                        continue
    
                    dataset.is_open = (ds_entry['is_open'] == 'y')
                    dataset.blocks.clear()
                    dataset.size = 0
                    dataset.num_files = 0

                    # start from the full list of blocks and files and remove ones found in PhEDEx
                    invalidated_blocks = set(dataset.blocks)

                    for block_entry in ds_entry['block']:
                        try:
                            block_name = Block.translate_name(block_entry['name'].replace(dataset.name + '#', ''))
                        except:
                            logger.error('Invalid block name %s in data', block_entry['name'])
                            continue
    
                        block = dataset.find_block(block_name)
    
                        if block is None:
                            block = Block(
                                block_name,
                                dataset = dataset,
                                size = block_entry['bytes'],
                                num_files = block_entry['files'],
                                is_open = (block_entry['is_open'] == 'y')
                            )
                            block.files = set()

                            dataset.blocks.add(block)

                            dataset.size += block.size
                            dataset.num_files += block.num_files
    
                        else:
                            invalidated_blocks.remove(block)
                            block.size = block_entry['bytes']
                            block.num_files = block_entry['files']
                            block.is_open = (block_entry['is_open'] == 'y')
                            if block.files is None:
                                block.files = set()

                        if block_entry['time_update'] is not None and int(block_entry['time_update']) > dataset.last_update:
                            dataset.last_update = int(block_entry['time_update'])

                        invalidated_files = dict((f.lfn, f) for f in block.files)
                        for file_entry in block_entry['file']:
                            try:
                                lfile = invalidated_files.pop(file_entry['lfn'])
                            except KeyError:
                                block.files.add(File(file_entry['lfn'], block, file_entry['size']))
                            else:
                                lfile.size = file_entry['size']

                        for lfile in invalidated_files.itervalues():
                            block.remove_file(lfile)

                    for block in invalidated_blocks:
                        logger.info('Removing block %s from dataset %s', block.real_name(), dataset.name)
                        dataset.remove_block(block)

        # set_constituent can take 10000 datasets at once, make it smaller and more parallel
        chunk_size = 100
        dataset_chunks = []

        start = 0
        while start < len(datasets):
            dataset_chunks.append(datasets[start:start + chunk_size])
            start += chunk_size

        parallel_exec(inquire_phedex, dataset_chunks, num_threads = 64, print_progress = (len(datasets) > 1000))

    def _set_dataset_status_and_type(self, datasets):
        """
        Use DBS 'datasetlist' to set dataset status and type.
        Called by fill_dataset_info to inquire about production/unknown datasets,
        or by set_dataset_details for a full scan.
        Argument is a list of datasets.
        """

        def inquire_dbs(dataset_list):
            # here threads are genuinely independent - no lock required

            names = [d.name for d in dataset_list]

            # We may have an invalid dataset within the list, in which case the entire call will be scrapped.
            # In such cases, we need to identify and throw out the invalid datasets recursively.

            try:
                dbs_response = self._make_dbs_request('datasetlist', {'dataset': names, 'detail': True}, method = POST, format = 'json')
            except:
                if self._dbs_interface.last_errorcode == 400:
                    if len(dataset_list) == 1:
                        dataset = dataset_list[0]
                        logger.debug('set_dataset_details  DBS throws an error on %s.', dataset.name)
                        # this dataset is in PhEDEx but not in DBS - set to IGNORED and clean them up regularly
                        dataset.status = Dataset.STAT_UNKNOWN
                        dataset.data_type = Dataset.TYPE_UNKNOWN
                    else:
                        # split the list in half and inquire DBS for each
                        inquire_dbs(dataset_list[:len(dataset_list) / 2])
                        inquire_dbs(dataset_list[len(dataset_list) / 2:])

                    return

                else:
                    logger.error('DBS error: %s', str(self._dbs_interface.last_exception))
                    raise RuntimeError('set_dataset_status_and_type')
                    
            # normal return
            result = dict((e['dataset'], e) for e in dbs_response)
    
            for dataset in dataset_list:
                try:
                    dbs_entry = result[dataset.name]
                except KeyError:
                    logger.debug('set_dataset_details  %s is not in DBS.', dataset.name)
                    # this dataset is in PhEDEx but not in DBS - set to UNKNOWN
                    # We used to set the status to IGNORED, but this would cause problems
                    # with very new datasets.
                    dataset.status = Dataset.STAT_UNKNOWN
                    dataset.data_type = Dataset.TYPE_UNKNOWN
                    continue
    
                dataset.status = Dataset.status_val(dbs_entry['dataset_access_type'])
                dataset.data_type = Dataset.data_type_val(dbs_entry['primary_ds_type'])
                if int(dbs_entry['last_modification_date']) > dataset.last_update:
                    # normally last_update is determined by the last block update
                    # in case there was a change in the dataset info itself in DBS
                    dataset.last_update = int(dbs_entry['last_modification_date'])


        # set_status_type can work on up to 1000 datasets, but the http POST seems not able to handle huge inputs
        chunk_size = 300
        dataset_chunks = []

        start = 0
        while start < len(datasets):
            dataset_chunks.append(datasets[start:start + chunk_size])
            start += chunk_size

        parallel_exec(inquire_dbs, dataset_chunks, print_progress = (len(datasets) > 1000))

    def _set_software_version_info(self, datasets):
        """
        Use DBS 'releaseversions' to set software versions of datasets.
        Argument is a list of datasets.
        """

        def inquire_dbs(dataset):
            logger.debug('set_dataset_software_info  Fetching software version for %s', dataset.name)

            result = self._make_dbs_request('releaseversions', ['dataset=' + dataset.name])
            if len(result) == 0 or 'release_version' not in result[0]:
                return
    
            # a dataset can have multiple versions; use the first one
            version = result[0]['release_version'][0]

            matches = re.match('CMSSW_([0-9]+)_([0-9]+)_([0-9]+)(|_.*)', version)
            if matches:
                cycle, major, minor = map(int, [matches.group(i) for i in range(1, 4)])
    
                if matches.group(4):
                    suffix = matches.group(4)[1:]
                else:
                    suffix = ''
    
                dataset.software_version = (cycle, major, minor, suffix)

        parallel_exec(inquire_dbs, datasets, print_progress = (len(datasets) > 1000))

    def _make_phedex_request(self, resource, options = [], method = GET, format = 'url', raw_output = False):
        """
        Make a single PhEDEx request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._phedex_interface.make_request(resource, options = options, method = method, format = format, cache_lifetime = config.phedex.cache_lifetime)

        try:
            result = resp['phedex']
        except KeyError:
            logger.error(resp)
            return

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(pprint.pformat(result))

        if raw_output:
            return result

        for metadata in ['request_timestamp', 'instance', 'request_url', 'request_version', 'request_call', 'call_time', 'request_date']:
            result.pop(metadata)

        # the only one item left in the results should be the result body. Clone the keys to use less memory..
        key = result.keys()[0]
        body = result[key]
        
        return body

    def _make_dbs_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single DBS request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._dbs_interface.make_request(resource, options = options, method = method, format = format)

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(pprint.pformat(resp))

        return resp

    def _form_catalog_xml(self, file_catalogs, human_readable = False):
        """
        Take a catalog dict of form {dataset: [block]} and form an input xml for delete and subscribe calls.
        """

        # we should consider using an actual xml tool
        if human_readable:
            xml = '<data version="2.0">\n <dbs name="%s">\n' % config.dbs.url_base
        else:
            xml = '<data version="2.0"><dbs name="%s">' % config.dbs.url_base

        for dataset, blocks in file_catalogs.iteritems():
            if human_readable:
                xml += '  '

            xml += '<dataset name="{name}" is-open="{is_open}">'.format(name = dataset.name, is_open = ('y' if dataset.is_open else 'n'))

            if human_readable:
                xml += '\n'

            for block in blocks:
                block_name = dataset.name + '#' + block.real_name()

                if human_readable:
                    xml += '   '
                
                xml += '<block name="{name}" is-open="{is_open}"/>'.format(name = block_name, is_open = ('y' if block.is_open else 'n'))
                if human_readable:
                    xml += '\n'

            if human_readable:
                xml += '  '

            xml += '</dataset>'

            if human_readable:
                xml += '\n'

        if human_readable:
            xml += ' </dbs>\n</data>\n'
        else:
            xml += '</dbs></data>'

        return xml
