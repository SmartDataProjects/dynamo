import logging
import time
import re
import collections
import pprint
import fnmatch
import threading

from common.interface.copy import CopyInterface
from common.interface.deletion import DeletionInterface
from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.replicainfo import ReplicaInfoSourceInterface
from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from common.dataformat import Dataset, Block, File, Site, Group, DatasetReplica, BlockReplica
from common.misc import parallel_exec
import common.configuration as config

logger = logging.getLogger(__name__)

# Using POST requests with PhEDEx:
# Accumulate dataset=/A/B/C options and make a query once every 10000 entries
# PhEDEx does not document a hard limit on the length of POST request list.
# 10000 was experimentally verified to be OK.

class PhEDExDBSSSB(CopyInterface, DeletionInterface, SiteInfoSourceInterface, ReplicaInfoSourceInterface, DatasetInfoSourceInterface):
    """
    Interface to PhEDEx/DBS/SSB using datasvc REST API.
    """

    def __init__(self, phedex_url = config.phedex.url_base, dbs_url = config.dbs.url_base, ssb_url = config.ssb.url_base):
        CopyInterface.__init__(self)
        DeletionInterface.__init__(self)
        SiteInfoSourceInterface.__init__(self)
        ReplicaInfoSourceInterface.__init__(self)
        DatasetInfoSourceInterface.__init__(self)

        self._phedex_interface = RESTService(phedex_url, use_cache = True)
        self._dbs_interface = RESTService(dbs_url) # needed for detailed dataset info
        self._ssb_interface = RESTService(ssb_url) # needed for site status

        self._last_request_time = 0
        self._last_request_url = ''

    def schedule_copy(self, dataset_replica, group, comments = '', auto_approval = True, is_test = False): #override (CopyInterface)
        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        dataset = dataset_replica.dataset
        replica_blocks = [r.block for r in dataset_replica.block_replicas]

        # shouldn't pass datasets with blocks not loaded though..
        if dataset.blocks is not None and set(replica_blocks) == set(dataset.blocks):
            catalogs[dataset] = []
            level = 'dataset'
        else:
            catalogs[dataset] = replica_blocks
            level = 'block'

        options = {
            'node': dataset_replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': level,
            'priority': 'normal',
            'move': 'n',
            'static': 'n',
            'custodial': 'n',
            'group': group.name,
            'request_only': 'n',
            'no_mail': 'n',
            'comments': comments
        }

        logger.info('schedule_copy  subscribe %d datasets at %s', len(catalogs), options['node'])
        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug('schedule_copy  subscribe: %s', str(options))

        if config.read_only:
            return

        if is_test:
            return -1

        else:
            result = self._make_phedex_request('subscribe', options, method = POST)
    
            if len(result) == 0:
                logger.error('schedule_copy failed.')
                return 0
    
            return int(result[0]['id'])

    def schedule_copies(self, replicas, group, comments = '', auto_approval = True, is_test = False): #override (CopyInterface)
        request_mapping = {}

        def run_subscription_request(site, replica_list):
            # replica_list can contain DatasetReplica and BlockReplica mixed

            catalogs = collections.defaultdict(list)

            level = 'dataset'

            for replica in replica_list:
                if type(replica) is DatasetReplica:
                    dataset = replica.dataset
                    replica_blocks = [r.block for r in replica.block_replicas]

                    if dataset.blocks is not None and set(replica_blocks) == set(dataset.blocks):
                        catalogs[dataset] = []
                    else:
                        catalogs[dataset].extend(replica_blocks)
                        level = 'block'

                elif type(replica) is BlockReplica:
                    catalogs[replica.block.dataset].append(replica.block)
                    level = 'block'

            options = {
                'node': site.name,
                'data': self._form_catalog_xml(catalogs),
                'level': level,
                'priority': 'normal',
                'move': 'n',
                'static': 'n',
                'custodial': 'n',
                'group': group.name,
                'request_only': 'n',
                'no_mail': 'n',
                'comments': comments
            }

            logger.info('schedule_copies  subscribe %d datasets at %s', len(catalogs), options['node'])
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug('schedule_copies  subscribe: %s', str(options))

            if config.read_only:
                return

            if is_test:
                request_id = -1
                while request_id in request_mapping:
                    request_id -= 1

                request_mapping[request_id] = (True, replica_list)

            else:
                # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
                result = self._make_phedex_request('subscribe', options, method = POST)
    
                if len(result) == 0:
                    logger.error('schedule_copies  copy failed.')
                    return
    
                request_id = int(result[0]['id']) # return value is a string
    
                logger.warning('PhEDEx subscription request id: %d', request_id)
                
                request_mapping[request_id] = (True, replica_list)

        replicas_by_site = collections.defaultdict(list)
        for replica in replicas:
            replicas_by_site[replica.site].append(replica)

        for site, replica_list in replicas_by_site.items():
            subscription_chunk = []
            chunk_size = 0
            for replica in replica_list:
                subscription_chunk.append(replica)
                if type(replica) is DatasetReplica:
                    chunk_size += replica.size(physical = False)
                elif type(replica) is BlockReplica:
                    chunk_size += replica.block.size

                if chunk_size >= config.phedex.subscription_chunk_size or replica == replica_list[-1]:
                    run_subscription_request(site, subscription_chunk)
                    subscription_chunk = []
                    chunk_size = 0

        return request_mapping

    def schedule_reassignments(self, replicas, group, comments = '', auto_approval = True, is_test = False): #override (CopyInterface)
        # for PhEDEx, copying and ownership reassignment are the same thing
        self.schedule_copies(replicas, group, comments, auto_approval, is_test)

    def schedule_deletion(self, replica, comments = '', auto_approval = True, is_test = False): #override (DeletionInterface)
        if replica.site.storage_type == Site.TYPE_MSS and config.daemon_mode:
            logger.warning('Deletion from MSS cannot be done in daemon mode.')
            return None

        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        if type(replica) == DatasetReplica:
            replica_blocks = [r.block for r in replica.block_replicas]

            if replica.dataset.blocks is not None and set(replica_blocks) == set(replica.dataset.blocks):
                catalogs[replica.dataset] = []
                level = 'dataset'
            else:
                catalogs[replica.dataset] = replica_blocks
                level = 'block'

        elif type(replica) == BlockReplica:
            catalogs[replica.block.dataset] = [replica.block]
            level = 'block'

        options = {
            'node': replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': level,
            'rm_subscriptions': 'y',
            'comments': comments
        }

        if config.read_only:
            logger.info('schedule_deletion  delete %d datasets', len(catalogs))
            logger.debug('schedule_deletion  delete: %s', str(options))
            return None

        if is_test:
            logger.info('schedule_deletion  delete %d datasets', len(catalogs))
            logger.debug('schedule_deletion  delete: %s', str(options))
            return (-1, True, [replica])

        else:
            try:
                result = self._make_phedex_request('delete', options, method = POST)
            except:
                logger.error('schedule_deletion  delete failed.')
                return (0, False, [])

            request_id = int(result[0]['id']) # return value is a string

            logger.warning('PhEDEx deletion request id: %d', request_id)

            return_value = (request_id, False, [replica])

            if auto_approval:
                try:
                    result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': replica.site.name}, method = POST)
                    return_value = (request_id, True, [replica])
                except:
                    logger.error('schedule_deletions  deletion approval failed.')

            return return_value

    def schedule_deletions(self, replica_list, comments = '', auto_approval = True, is_test = False): #override (DeletionInterface)
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        has_mss = False
        for replica in replica_list:
            replicas_by_site[replica.site].append(replica)
            if replica.site.storage_type == Site.TYPE_MSS:
                has_mss = True

        if has_mss and config.daemon_mode:
            logger.warning('Deletion from MSS cannot be done in daemon mode.')
            return {}

        def call_deletion(site, level, deletion_list):
            """
            Sometimes we have invalid data in the list of objects to delete.
            PhEDEx throws a 400 error in such a case. We have to then try to identify the
            problematic item through trial and error.
            """

            catalogs = {}
            for replica in deletion_list:
                if level == 'dataset':
                    catalogs[replica.dataset] = []
                elif level == 'block':
                    catalogs[replica.dataset] = [r.block for r in replica.block_replicas]

            if len(catalogs) == 0:
                return

            options = {
                'node': site.name,
                'data': self._form_catalog_xml(catalogs),
                'level': level,
                'rm_subscriptions': 'y',
                'comments': comments
            }

            if config.read_only:
                logger.info('schedule_deletions  delete %d datasets', len(catalogs))
                logger.debug('schedule_deletions  delete: %s', str(options))
                return

            if is_test:
                logger.info('schedule_deletions  delete %d datasets', len(catalogs))
                logger.debug('schedule_deletions  delete: %s', str(options))
                request_id = -1
                while request_id in request_mapping:
                    request_id -= 1

                request_mapping[request_id] = (True, deletion_list)
                return

            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response) if successful
            try:
                result = self._make_phedex_request('delete', options, method = POST)
            except:
                if self._phedex_interface.last_errorcode == 400:
                    # bad request - split the deletion list and try each half
                    if len(deletion_list) == 1:
                        logger.error('schedule_deletions  Could not delete %s from %s', replica.dataset.name, site.name)
                    else:
                        call_deletion(site, level, deletion_list[:len(deletion_list) / 2])
                        call_deletion(site, level, deletion_list[len(deletion_list) / 2:])
                else:
                    logger.error('schedule_deletions  Could not delete %d datasets from %s', len(deletion_list), site.name)
                    
                return

            request_id = int(result[0]['id']) # return value is a string
        
            request_mapping[request_id] = (False, deletion_list) # (completed, deleted_replicas)
        
            logger.warning('PhEDEx deletion request id: %d', request_id)
    
            if auto_approval:
                try:
                    result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)
                    request_mapping[request_id] = (True, deletion_list)
                except:
                    logger.error('schedule_deletions  deletion approval failed.')
            

        for site, replica_list in replicas_by_site.items():
            # execute the deletions in two steps: one for dataset-level and one for block-level
            deletion_lists = {'dataset': [], 'block': []}

            for replica in replica_list:
                replica_blocks = [r.block for r in replica.block_replicas]

                if replica.dataset.blocks is not None and set(replica_blocks) == set(replica.dataset.blocks):
                    deletion_lists['dataset'].append(replica)
                else:
                    deletion_lists['block'].append(replica)

            call_deletion(site, 'dataset', deletion_lists['dataset'])
            call_deletion(site, 'block', deletion_lists['block'])

        return request_mapping

    def copy_status(self, request_id): #override (CopyInterface)
        request = self._make_phedex_request('transferrequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        site_name = request[0]['destinations']['node'][0]['name']

        dataset_names = []
        for ds_entry in request[0]['data']['dbs']['dataset']:
            dataset_names.append(ds_entry['name'])

        block_names = []
        for ds_entry in request[0]['data']['dbs']['block']:
            block_names.append(ds_entry['name'].replace('#', '%23'))

        subscriptions = []

        if len(dataset_names) != 0:
            chunks = [dataset_names[i:i + 35] for i in xrange(0, len(dataset_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._make_phedex_request('subscriptions', ['node=%s' % site_name] + ['dataset=%s' % n for n in chunk]))

        if len(block_names) != 0:
            chunks = [block_names[i:i + 35] for i in xrange(0, len(block_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._make_phedex_request('subscriptions', ['node=%s' % site_name] + ['block=%s' % n for n in chunk]))

        status = {}
        for dataset in subscriptions:
            try:
                cont = dataset['subscription'][0]
                bytes = dataset['bytes']
                node_bytes = cont['node_bytes']
                time_update = cont['time_update']
            except KeyError:
                # this was a block-level subscription (no 'subscription' field for the dataset)
                bytes = 0
                node_bytes = 0
                time_update = 0
                for block in dataset['block']:
                    cont = block['subscription'][0]
                    bytes += block['bytes']
                    node_bytes += cont['node_bytes']
                    time_update = max(time_update, cont['time_update'])

            status[(site_name, dataset['name'])] = (bytes, node_bytes, time_update)

        return status

    def deletion_status(self, request_id): #override (DeletionInterface)
        request = self._make_phedex_request('deleterequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        node_info = request[0]['nodes']['node'][0]
        site_name = node_info['name']
        last_update = node_info['decided_by']['time_decided']

        status = {}
        for ds_entry in request[0]['data']['dbs']['dataset']:
            status[ds_entry['name']] = (ds_entry['bytes'], ds_entry['bytes'], last_update)
            
        return status

    def get_site_list(self, sites, include = ['*'], exclude = []): #override (SiteInfoSourceInterface)
        options = []
        if len(include) == 0:
            return

        if len(include) > 1 or include[0] != '*':
            options = ['node=%s' % s for s in include]

        logger.info('get_site_list  Fetching the list of nodes from PhEDEx')
        source = self._make_phedex_request('nodes', options)

        for entry in source:
            if entry['name'] not in sites and entry['name'] not in exclude:
                site = Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology'])
                sites[entry['name']] = site
        
    def set_site_status(self, sites): #override (SiteInfoSourceInterface)
        logger.info('set_site_status  Fetching the site status from SSB')

        for site in sites.values():
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

    def get_group_list(self, groups, filt = '*'): #override (SiteInfoSourceInterface)
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

    def make_replica_links(self, inventory, site_filt = '*', group_filt = '*', dataset_filt = '/*/*/*'): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site.
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

        logger.info('make_replica_links  Fetching block replica information from PhEDEx')

        lock = threading.Lock()
        counters = {
            'new_datasets': 0,
            'datasets_with_new_blocks': 0,
            'datasets_with_updated_blocks': 0,
            'datasets_with_updated_blocklist': 0,
            'new_blocks': 0
        }

        def exec_get(site_list, gname_list, dname_list):
            if len(site_list) == 1:
                logger.debug('Fetching replica info on %s.', site_list[0].name)

            options = ['show_dataset=y']
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
                        dataset = inventory.store.load_dataset(ds_name, load_blocks = True, load_files = False)
                        if dataset is None:
                            dataset = Dataset(ds_name, status = Dataset.STAT_PRODUCTION)
                            new_dataset = True
                            counters['new_datasets'] += 1

                        inventory.datasets[ds_name] = dataset

                    if dataset.blocks is None:
                        inventory.store.load_blocks(dataset)

                    dataset.is_open = (dataset_entry['is_open'] == 'y')

                    if dataset.blocks is None:
                        dataset.blocks = []
                        dataset.size = 0
                        dataset.num_files = 0

                    if dataset.replicas is None:
                        dataset.replicas = []

                    dataset_replica = None

                    new_block = False
                    updated_block = False

                    for block_entry in dataset_entry['block']:
                        try:
                            block_name = Block.translate_name(block_entry['name'].replace(ds_name + '#', ''))
                        except:
                            logger.error('Invalid block name %s in blockreplicas', ds_name)
                            continue

                        block = None
                        if not new_dataset:
                            block = dataset.find_block(block_name)

                        if block is None:
                            block = Block(
                                block_name,
                                dataset = dataset,
                                size = block_entry['bytes'],
                                num_files = block_entry['files'],
                                is_open = (block_entry['is_open'] == 'y')
                            )

                            dataset.blocks.append(block)
                            dataset.size += block.size
                            dataset.num_files += block.num_files
                            if dataset.status == Dataset.STAT_VALID:
                                # there are some pretty crazy cases with ignored datasets. We've seen cases like two datasets with identical name, each
                                # with its own list of blocks where PhEDEx "blockreplicas" and "data" and DBS "blocks" all don't agree
                                dataset.status = Dataset.STAT_PRODUCTION # trigger DBS query
                            
                                counters['new_blocks'] += 1
                                new_block = True

                        elif block.size != block_entry['bytes'] or block.num_files != block_entry['files'] or block.is_open != (block_entry['is_open'] == 'y'):
                            # block record was updated
                            block = dataset.update_block(block_name, block_entry['bytes'], block_entry['files'], (block_entry['is_open'] == 'y'))
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
                                dataset_replica = dataset.find_replica(site)

                            if dataset_replica is None:
                                # first time associating this dataset with this site
                                dataset_replica = DatasetReplica(
                                    dataset,
                                    site,
                                    is_complete = True,
                                    is_custodial = False,
                                    last_block_created = 0
                                )
    
                                dataset.replicas.append(dataset_replica)

                                site.dataset_replicas.add(dataset_replica)
    
                            if replica_entry['time_update'] > dataset_replica.last_block_created:
                                dataset_replica.last_block_created = replica_entry['time_update']

                            # PhEDEx 'complete' flag cannot be trusted; defining completeness in terms of size.
                            is_complete = (replica_entry['bytes'] == block.size)
                            is_custodial = (replica_entry['custodial'] == 'y')
    
                            # if any block replica is not complete, dataset replica is not
                            if not is_complete:
                                dataset_replica.is_complete = False
    
                            # if any of the block replica is custodial, dataset replica also is
                            if is_custodial:
                                dataset_replica.is_custodial = True
    
                            block_replica = BlockReplica(
                                block,
                                site,
                                group,
                                is_complete, 
                                is_custodial,
                                size = replica_entry['bytes']
                            )
    
                            dataset_replica.block_replicas.append(block_replica)

                            site.add_block_replica(block_replica)

                    if new_block:
                        counters['datasets_with_new_blocks'] += 1
                    if updated_block:
                        counters['datasets_with_updated_blocks'] += 1

        all_sites = [site for name, site in inventory.sites.items() if fnmatch.fnmatch(name, site_filt)]
        gname_list = [name for name in inventory.groups.keys() if fnmatch.fnmatch(name, group_filt)] + [None]

        if dataset_filt == '/*/*/*' or dataset_filt == '' or dataset_filt == '*':
            items = []
            for site in all_sites:
                total_quota = site.quota()
                if total_quota >= 500:
                    # further split by the first character of the dataset names
                    # split depending on the quota
                    characters = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
                    chunk_size = max(len(characters) / int(total_quota / 100), 1)
                    charsets = [characters[i:i + chunk_size] for i in range(0, len(characters), chunk_size)]
                    for charset in charsets:
                        items.append(([site], gname_list, ['/%s*/*/*' % c for c in charset]))
                else:
                    items.append(([site], gname_list, ['/*/*/*']))

            parallel_exec(exec_get, items, num_threads = min(64, len(items)), print_progress = True, timeout = 3600)
            del items
        else:
            exec_get(all_sites, gname_list, [dataset_filt])

        logger.info('Checking dataset status changes.')

        invalid_or_deprecated = set(d for d in inventory.datasets.values() if d.status == Dataset.STAT_INVALID or d.status == Dataset.STAT_DEPRECATED)

        dbs_invalids = self._make_dbs_request('datasets', ['dataset_access_type=INVALID'])
        for ds_entry in dbs_invalids:
            try:
                dataset = inventory.datasets[ds_entry['dataset']]
            except KeyError:
                continue

            dataset.status = Dataset.STAT_INVALID

            try:
                invalid_or_deprecated.remove(dataset)
            except KeyError:
                pass

        dbs_deprecated = self._make_dbs_request('datasets', ['dataset_access_type=DEPRECATED'])
        for ds_entry in dbs_deprecated:
            try:
                dataset = inventory.datasets[ds_entry['dataset']]
            except KeyError:
                continue

            dataset.status = Dataset.STAT_DEPRECATED

            try:
                invalid_or_deprecated.remove(dataset)
            except KeyError:
                pass

        # remaining datasets in the list must have been revalidated
        # set it to production to trigger further inspection
        for dataset in invalid_or_deprecated:
            logger.info('%s was invalid or deprecated but not any more', dataset.name)
            dataset.status = Dataset.STAT_PRODUCTION

        logger.info('Checking for updated datasets.')

        for dataset in inventory.datasets.values():
            if dataset.status != Dataset.STAT_VALID:
                # dataset is already marked for further inspection or ignored
                continue

            if dataset.blocks is None or dataset.replicas is None:
                # dataset is not loaded up
                continue

            # check for potentially invalidated blocks
            blocks_with_replicas = set()
            for replica in dataset.replicas:
                blocks_with_replicas.update([r.block for r in replica.block_replicas])

            if blocks_with_replicas != set(dataset.blocks):
                counters['datasets_with_updated_blocklist'] += 1
                dataset.status = Dataset.STAT_PRODUCTION # trigger DBS query

        logger.info('Done.')
        logger.info(' %d new datasets', counters['new_datasets'])
        logger.info(' %d new blocks', counters['new_blocks'])
        logger.info(' %d datasets with new blocks', counters['datasets_with_new_blocks'])
        logger.info(' %d datasets with updated blocks', counters['datasets_with_updated_blocks'])
        logger.info(' %d datasets with updated blocklist', counters['datasets_with_updated_blocklist'])

    def find_tape_copies(self, datasets): #override (ReplicaInfoSourceInterface)
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
                dataset = datasets[ds_entry['name']]
 
                ds_bytes = ds_entry['bytes']
 
                # if a dataset-level or block-level subscription exists, it's at least partial
 
                if 'subscription' in ds_entry:
                    if len(ds_entry['subscription']) != 0:
                        dataset.on_tape = Dataset.TAPE_PARTIAL
 
                    for ds_subscription in ds_entry['subscription']:
                        if ds_subscription['node_bytes'] == ds_bytes:
                            dataset.on_tape = Dataset.TAPE_FULL
                            break
     
                    else: # else of for
                        # This dataset is fully on tape. No more need to process data
                        continue
  
                if 'block' in ds_entry:
                    if len(ds_entry['block']) != 0:
                        dataset.on_tape = Dataset.TAPE_PARTIAL
  
                    block_names = set(b.name for b in dataset.blocks)
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
                            if bl_subscription['node_bytes'] == bl_bytes:
                                blocks_at_sites[bl_subscription['node']].add(block_name)
     
                    for names in blocks_at_sites.values():
                        if names == block_names:
                            dataset.on_tape = Dataset.TAPE_FULL
                            break
 
        chunk_size = 1000
        dataset_chunks = [[]]

        # Loop over datasets not on tape
        for dataset in datasets.values():
           # on_tape is TAPE_NONE by default
           if dataset.on_tape == Dataset.TAPE_FULL:
               continue

           if dataset.status != Dataset.STAT_VALID and dataset.status != Dataset.STAT_PRODUCTION:
               continue

           if dataset.blocks is None:
               # this dataset is not loaded at the moment
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

        release_unknown = filter(lambda d: d.software_version is None, datasets)

        logger.info('set_dataset_details  Finding the software version for %d datasets.', len(release_unknown))

        self._set_software_version_info(release_unknown)

    def _set_dataset_constituent_info(self, datasets):
        """
        Query phedex "data" interface and fill the list of blocks.
        Argument is a list of datasets.
        """

        lock = threading.Lock()

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
                        # This function is called after make_replica_ links
                        # i.e. "blockreplicas" knows about this dataset but "data" doesn't.
                        # i.e. something is screwed up.
                        # We used to set the status to IGNORED, but this would cause problems
                        # with very new datasets.
                        dataset.is_open = True
                        dataset.status = Dataset.STAT_UNKNOWN
                        continue
    
                    dataset.is_open = (ds_entry['is_open'] == 'y')

                    if dataset.blocks is None:
                        dataset.blocks = []
                        dataset.size = 0
                        dataset.num_files = 0

                    # start from the full list of blocks and files and remove ones found in PhEDEx
                    invalidated_blocks = set(dataset.blocks)

                    files = [] # list of (lfn, block, size)

                    for block_entry in ds_entry['block']:
                        try:
                            block_name = Block.translate_name(block_entry['name'].replace(dataset.name + '#', ''))
                        except:
                            logger.error('Invalid block name %s in data', block_name)
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
                            dataset.blocks.append(block)

                            dataset.size += block.size
                            dataset.num_files += block.num_files
    
                        else:
                            invalidated_blocks.remove(block)
                            if block.size != block_entry['bytes'] or block.num_files != block_entry['files'] or block.is_open != (block_entry['is_open'] == 'y'):
                                block = dataset.update_block(block_name, block_entry['bytes'], block_entry['files'], (block_entry['is_open'] == 'y'))

                        if block_entry['time_update'] is not None and int(block_entry['time_update']) > dataset.last_update:
                            dataset.last_update = int(block_entry['time_update'])

                        for file_entry in block_entry['file']:
                            files.append((file_entry['lfn'], block, file_entry['size']))

                    for block in invalidated_blocks:
                        logger.info('Removing block %s from dataset %s', block.real_name(), dataset.name)
                        dataset.remove_block(block)

                    if dataset.files is None:
                        dataset.files = set()
                        for file_info in files:
                            dataset.files.add(File.create(*file_info))

                    else:
                        # file structure already exists for the dataset. compare to query results and update.

                        files.sort()
    
                        # files in invalidated blocks are already removed by Dataset.remove_block()
                        known_files = sorted(dataset.files, key = lambda f: f.fullpath())
    
                        invalidated_files = []
    
                        # compare two sorted lists side-by-side
                        isource = 0
                        iknown = 0
                        while True:
                            if isource == len(files):
                                # no more from phedex; rest is known but invalidated
                                invalidated_files.extend(known_files[iknown:])
                                break
    
                            elif iknown == len(known_files):
                                # all remaining files are new
                                for file_info in files[isource:]:
                                    dataset.files.add(File.create(*file_info))
                                break
    
                            else:
                                phed_name, block, phed_size = files[isource]
                                known_file = known_files[iknown]
                                known_path = known_file.fullpath()
    
                                pathcmp = cmp(phed_name, known_path)
    
                                if pathcmp == 0:
                                    # same file
                                    if phed_size != known_file.size:
                                        dataset.update_file(phed_name, phed_size)
        
                                    isource += 1
                                    iknown += 1
    
                                elif pathcmp < 0:
                                    # new file
                                    dataset.files.add(File.create(phed_name, block, phed_size))
                                    isource += 1
    
                                else:
                                    # invalidated file
                                    invalidated_files.append(known_file)
                                    iknown += 1
    
                        for lfile in invalidated_files:
                            logger.info('Removing file %s from dataset %s', lfile.fullpath(), dataset.name)
                            dataset.files.remove(lfile)


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
    
            dbs_entries = self._make_dbs_request('datasetlist', {'dataset': names, 'detail': True}, method = POST, format = 'json')
            result = dict((e['dataset'], e) for e in dbs_entries)
    
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

        self._last_request = result['request_timestamp']
        self._last_request_url = result['request_url']

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

        for dataset, blocks in file_catalogs.items():
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


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'PhEDEx interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '*', default = [], help = 'Option string as passed to PhEDEx datasvc.')
    parser.add_argument('--url', '-u', dest = 'phedex_url', metavar = 'URL', default = config.phedex.url_base, help = 'PhEDEx URL base.')
    parser.add_argument('--method', '-m', dest = 'method', metavar = 'METHOD', default = 'GET', help = 'HTTP method.')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')
    parser.add_argument('--raw', '-A', dest = 'raw_output', action = 'store_true', help = 'Print RAW PhEDEx response.')
    parser.add_argument('--test', '-T', dest = 'is_test', action = 'store_true', help = 'Test mode for commands delete and subscribe.')
 
    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    command = args.command

    interface = PhEDExDBSSSB(phedex_url = args.phedex_url)

    if args.method == 'POST':
        method = POST
    else:
        method = GET

    options = args.options

    if command == 'delete' or command == 'subscribe':
        method = POST

        if not args.is_test and (args.phedex_url == config.phedex.url_base or 'prod' in args.phedex_url):
            print 'Are you sure you want to run this command on a prod instance? [Y/n]'
            response = sys.stdin.readline().strip()
            if response != 'Y':
                sys.exit(0)

        site = None
        group = None
        datasets = []
        blocks = {}
        comments = ''
        for io in xrange(len(args.options)):
            opt = args.options[io]
            matches = re.match('(node|group|dataset|block|comments)=(.+)', opt)
            if not matches:
                print 'Invalid argument ' + opt
                sys.exit(1)

            key = matches.group(1)
            value = matches.group(2)

            if key == 'node':
                site = Site(value)
            elif key == 'group':
                group = Group(value)
            elif key == 'dataset':
                if not re.match('/[^/]+/[^/]+/[^/]+', value):
                    print 'Invalid dataset name ' + value
                    sys.exit(1)

                if value not in datasets:
                    datasets.append(value)
            elif key == 'block':
                if '#' in value:
                    dname, bnane = value.split('#')
                else:
                    dname, bnane = value.split('%23')

                if not re.match('/[^/]+/[^/]+/[^/]+', dname):
                    print 'Invalid dataset name ' + dname
                    sys.exit(1)
    
                if len(bnane) != 36:
                    print 'Invalid block name ' + dname + '#' + bnane
                    sys.exit(1)

                try:
                    if bname not in blocks[dname]:
                        blocks[dname].append(bnane)
                except KeyError:
                    blocks[dname] = [bnane]

            elif key == 'comments':
                comments = value

        if site is None or (command == 'subscribe' and group is None) or (len(datasets) == 0 and len(blocks) == 0) or comments == '':
            print 'Must specify node, group, comments, and dataset or block'
            sys.exit(1)

        for dname in datasets:
            if dname in blocks:
                print 'Cannot make dataset-level and block-level requests of a same dataset.'
                sys.exit(1)

        replicas = []
        for dname in datasets:
            dataset = Dataset(dname)
            replicas.append(DatasetReplica(dataset, site))

        for dname, bnames in blocks.items():
            dataset = Dataset(dname)
            dataset_replica = DatasetReplica(dataset, site)
            replicas.append(dataset_replica)

            for bname in bnames:
                block = Block(Block.translate_name(bname), dataset, 0, 0, False)
                # don't add the block to dataset (otherwise will become a dataset-level operation)
                block_replica = BlockReplica(block, site, group, True, False, 0)
                dataset_replica.block_replicas.append(block_replica)

        print 'Replicas', replicas
        print 'Comments', comments
        print 'Confirm ' + command + ' [Y/n]'
        response = sys.stdin.readline().strip()
        if response != 'Y':
            sys.exit(0)

        if command == 'delete':
            interface.schedule_deletions(replicas, comments = comments, is_test = args.is_test)

        elif command == 'subscribe':
            interface.schedule_copies(replicas, group, comments = comments, is_test = args.is_test)

        sys.exit(0)

    elif command == 'updaterequest' or command == 'updatesubscription' or command == 'data':
        method = POST

    result = interface._make_phedex_request(command, options, method = method, raw_output = args.raw_output)

    if command == 'requestlist':
        result.sort(key = lambda x: x['id'])

    pprint.pprint(result)
