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
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
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

        self._phedex_interface = RESTService(phedex_url)
        self._dbs_interface = RESTService(dbs_url) # needed for detailed dataset info
        self._ssb_interface = RESTService(ssb_url) # needed for site status

        self._last_request_time = 0
        self._last_request_url = ''

    def schedule_copy(self, dataset_replica, group, comments = '', is_test = False): #override (CopyInterface)
        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        dataset = dataset_replica.dataset
        replica_blocks = [r.block for r in dataset_replica.block_replicas]

        if set(replica_blocks) == set(dataset.blocks):
            catalogs[dataset] = []
            level = 'dataset'
        else:
            catalogs[dataset] = replica_blocks
            level = 'block'

        options = {
            'node': dataset_replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': level,
            'priority': 'low',
            'move': 'n',
            'static': 'n',
            'custodial': 'n',
            'group': group.name,
            'request_only': 'n',
            'no_mail': 'n',
            'comments': comments
        }

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

    def schedule_copies(self, replicas, group, comments = '', is_test = False): #override (CopyInterface)
        request_mapping = {}

        def run_subscription_request(site, replica_list):
            catalogs = {}

            level = 'dataset'

            for drep in replica_list:
                dataset = drep.dataset
                replica_blocks = [r.block for r in drep.block_replicas]

                if set(replica_blocks) == set(dataset.blocks):
                    catalogs[dataset] = []
                else:
                    catalogs[dataset] = replica_blocks
                    level = 'block'

            options = {
                'node': site.name,
                'data': self._form_catalog_xml(catalogs),
                'level': level,
                'priority': 'low',
                'move': 'n',
                'static': 'n',
                'custodial': 'n',
                'group': group.name,
                'request_only': 'n',
                'no_mail': 'n',
                'comments': comments
            }

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
                chunk_size += replica.size(physical = False)
                if chunk_size >= config.phedex.subscription_chunk_size or replica == replica_list[-1]:
                    run_subscription_request(site, subscription_chunk)
                    subscription_chunk = []
                    chunk_size = 0

        return request_mapping

    def schedule_deletion(self, replica, comments = '', is_test = False): #override (DeletionInterface)
        if replica.site.storage_type == Site.TYPE_MSS and config.daemon_mode:
            logger.warning('Deletion from MSS cannot be done in daemon mode.')
            return None

        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        if type(replica) == DatasetReplica:
            catalogs[replica.dataset] = [block_replica.block for block_replica in replica.block_replicas]

        elif type(replica) == BlockReplica:
            catalogs[replica.block.dataset] = [replica.block]

        options = {
            'node': replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': 'dataset',
            'rm_subscriptions': 'y',
            'comments': comments
        }

        if config.read_only:
            logger.info('schedule_deletion  delete %d datasets', len(catalogs))
            logger.debug('schedule_deletion  delete: %s', str(options))
            return None

        if is_test:
            return (-1, True, [replica])

        else:
            try:
                result = self._make_phedex_request('delete', options, method = POST)
            except:
                logger.error('schedule_deletions  delete failed.')
                return (0, False, [])

            request_id = int(result[0]['id']) # return value is a string

            logger.warning('PhEDEx deletion request id: %d', request_id)

            try:
                result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': replica.site.name}, method = POST)
            except:
                logger.error('schedule_deletions  deletion approval failed.')
                return (request_id, False, [replica])

            return (request_id, True, [replica])

    def schedule_deletions(self, replica_list, comments = '', is_test = False): #override (DeletionInterface)
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

        for site, replica_list in replicas_by_site.items():
            catalogs = {}

            for replica in replica_list:
                catalogs[replica.dataset] = [block_replica.block for block_replica in replica.block_replicas]

            options = {
                'node': site.name,
                'data': self._form_catalog_xml(catalogs),
                'level': 'dataset',
                'rm_subscriptions': 'y',
                'comments': comments
            }

            if config.read_only:
                logger.debug('schedule_deletions  delete: %s', str(options))
                continue

            if is_test:
                request_id = -1
                while request_id in request_mapping:
                    request_id -= 1

                request_mapping[request_id] = (True, replica_list)

            else:
                # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
                try:
                    result = self._make_phedex_request('delete', options, method = POST)
                except:
                    logger.error('schedule_deletions  delete failed.')
                    return request_mapping
    
                request_id = int(result[0]['id']) # return value is a string
    
                request_mapping[request_id] = (False, replica_list) # (completed, deleted_replicas)
    
                logger.warning('PhEDEx deletion request id: %d', request_id)

                try:
                    result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)
                except:
                    logger.error('schedule_deletions  deletion approval failed.')
                    return request_mapping
    
                request_mapping[request_id] = (True, replica_list)

        return request_mapping

    def copy_status(self, request_id): #override (CopyInterface)
        request = self._make_phedex_request('transferrequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        site_name = request[0]['destinations']['node'][0]['name']
        dataset_names = []
        for ds_entry in request[0]['data']['dbs']['dataset']:
            dataset_names.append(ds_entry['name'])

        subscriptions = self._make_phedex_request('subscriptions', ['node=%s' % site_name] + ['dataset=%s' % n for n in dataset_names])

        status = {}
        for subscription in subscriptions:
            cont = subscription['subscription'][0]
            status[(site_name, subscription['name'])] = (subscription['bytes'], cont['node_bytes'], cont['time_update'])

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

    def get_site_list(self, sites, filt = '*'): #override (SiteInfoSourceInterface)
        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['node=' + filt]
        elif type(filt) is list:
            options = ['node=%s' % s for s in filt]

        logger.info('get_site_list  Fetching the list of nodes from PhEDEx')
        source = self._make_phedex_request('nodes', options)

        for entry in source:
            if entry['name'] not in sites:
                site = Site(entry['name'], host = entry['se'], storage_type = Site.storage_type_val(entry['kind']), backend = entry['technology'])
                sites[entry['name']] = site
        
    def set_site_status(self, sites): #override (SiteInfoSourceInterface)
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

    def make_replica_links(self, sites, groups, datasets, site_filt = '*', group_filt = '*', dataset_filt = '/*/*/*'): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site.
        sites, groups, filt are used to limit the query.
        Objects in sites and datasets should have replica information cleared.
        """

        logger.info('make_replica_links  Fetching block replica information from PhEDEx')

        lock = threading.Lock()

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

                    try:
                        dataset = datasets[ds_name]
                        new_dataset = False

                    except KeyError:
                        dataset = Dataset(ds_name)
                        datasets[ds_name] = dataset
                        new_dataset = True

                    dataset.is_open = (dataset_entry['is_open'] == 'y')

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
                            dataset.status = Dataset.STAT_PRODUCTION # trigger DBS query

                        elif block.size != block_entry['bytes'] or block.num_files != block_entry['files'] or block.is_open != (block_entry['is_open'] == 'y'):
                            # block record was updated
                            block = dataset.update_block(block_name, block_entry['bytes'], block_entry['files'], (block_entry['is_open'] == 'y'))
                            dataset.status = Dataset.STAT_PRODUCTION

                        for replica_entry in block_entry['replica']:
                            if replica_entry['group'] not in gname_list:
                                continue
    
                            if replica_entry['group'] is not None:
                                try:
                                    group = groups[replica_entry['group']]
                                except KeyError:
                                    logger.warning('Group %s for replica of block %s not registered.', replica_entry['group'], block.real_name())
                                    group = None
                            else:
                                group = None
    
                            site = sites[replica_entry['node']]

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
    
            if len(sites) == 1:
                logger.debug('Done processing PhEDEx data from %s', site_list[0].name)


        all_sites = [site for name, site in sites.items() if fnmatch.fnmatch(name, site_filt)]
        gname_list = [name for name in groups.keys() if fnmatch.fnmatch(name, group_filt)] + [None]

        if dataset_filt == '/*/*/*' or dataset_filt == '' or dataset_filt == '*':
            items = []
            for site in all_sites:
                total_quota = site.quota()
                if total_quota >= 500:
                    # further split by the first character of the dataset names
                    # a-zA-Z0-9 -> 62 characters; split depending on the quota
                    chunk_size = max(62 / int(total_quota / 100), 1)
                    characters = 'aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ0123456789'
                    charsets = [characters[i:i + chunk_size] for i in range(0, 62, chunk_size)]
                    for charset in charsets:
                        items.append(([site], gname_list, ['/%s*/*/*' % c for c in charset]))
                else:
                    items.append(([site], gname_list, ['/*/*/*']))

            parallel_exec(exec_get, items, num_threads = min(64, len(items)), print_progress = True)
            del items
        else:
            exec_get(all_sites, gname_list, [dataset_filt])

        logger.info('Merging dataset information.')
        # Data retrieval was split in groups. Now merge the group information.
        for site in all_sites:
            for replica in list(site.dataset_replicas):
                if site.storage_type == Site.TYPE_MSS and replica.is_full():
                    replica.dataset.on_tape = Dataset.TAPE_FULL

                for block_replica in replica.block_replicas:
                    if replica.group is None:
                        replica.group = block_replica.group
                        continue
                        
                    if block_replica.group != replica.group:
                        replica.group = None
                        break

    def find_tape_copies(self, datasets): #override (ReplicaInfoSourceInterface)
        # Use 'blockreplicasummary' query to check if all blocks of the dataset are on tape.
        # site=T*MSS -> tape

        blocks_on_tape = collections.defaultdict(list)
        lock = threading.Lock()

        # Routine to fetch data and fill the list of blocks on tape
        def inquire_phedex(dataset_list):
            options = [('create_since', '0'), ('node', 'T*MSS'), ('custodial', 'y'), ('complete', 'y')]
            options.extend([('dataset', dataset.name) for dataset in dataset_list])

            source = self._make_phedex_request('blockreplicasummary', options, method = POST)

            on_tape = collections.defaultdict(list)

            for block_entry in source:
                name = block_entry['name']
                ds_name = name[:name.find('#')]
                try:
                    block_name = Block.translate_name(name[name.find('#') + 1:])
                except:
                    logger.error('Invalid block name %s in blockreplicasummary', name)
                    continue

                on_tape[ds_name].append(block_name)

            with lock:
                for ds_name, block_names in on_tape.items():
                    blocks_on_tape[ds_name].extend(block_names)

        chunk_size = 1000
        dataset_chunks = [[]]

        # Loop over datasets not on tape
        for dataset in datasets.values():
            # on_tape is TAPE_NONE by default
            if dataset.on_tape == Dataset.TAPE_FULL or dataset.status == Dataset.STAT_IGNORED:
                continue

            dataset_chunks[-1].append(dataset)
            if len(dataset_chunks[-1]) == chunk_size:
                dataset_chunks.append([])

        if len(dataset_chunks[-1]) == 0:
            dataset_chunks.pop()

        logger.info('find_tape_copies  Checking tape copies.')

        parallel_exec(inquire_phedex, dataset_chunks)
        del dataset_chunks

        # Loop again and fill datasets
        for dataset in datasets.values():
            if dataset.on_tape == Dataset.TAPE_FULL or dataset.status == Dataset.STAT_IGNORED:
                continue

            dataset.on_tape = Dataset.TAPE_NONE

            if dataset.name not in blocks_on_tape:
                continue

            on_tape = set(blocks_on_tape[dataset.name])

            dataset_blocks = set(b.name for b in dataset.blocks)
            if dataset_blocks == on_tape:
                dataset.on_tape = Dataset.TAPE_FULL
            else:
                # tape subscription is made, but is not complete
                dataset.on_tape = Dataset.TAPE_PARTIAL

    def set_dataset_details(self, datasets, skip_valid = False): #override (DatasetInfoSourceInterface)
        """
        Argument datasets is a {name: dataset} dict.
        skip_valid is True for routine inventory update.
        """

        if skip_valid:
            open_datasets = [dataset for dataset in datasets.values() if dataset.status == Dataset.STAT_PRODUCTION or dataset.status == Dataset.STAT_UNKNOWN]
        else:
            open_datasets = [dataset for dataset in datasets.values() if dataset.status == Dataset.STAT_PRODUCTION or dataset.status == Dataset.STAT_UNKNOWN or dataset.status == Dataset.STAT_VALID]

        logger.info('set_dataset_details  Finding blocks for %d datasets.', len(open_datasets))

        self._set_dataset_constituent_info(open_datasets)

        for dataset in list(open_datasets):
            if len(dataset.blocks) == 0:
                logger.info('get_datasets %s does not have any blocks and is removed.', dataset.name)
                datasets.pop(dataset.name)
                dataset.unlink()
                open_datasets.remove(dataset)

        logger.info('set_dataset_details  Setting status of %d datasets.', len(open_datasets))

        # DBS 'datasetlist' query. Sets not just the status but also the dataset type.
        self._set_dataset_status_and_type(open_datasets)

        release_unknown = [dataset for dataset in open_datasets if dataset.software_version[0] == 0]

        logger.info('set_dataset_details  Finding the software version for %d datasets.', len(release_unknown))

        self._set_software_version_info(release_unknown)

    def _set_dataset_constituent_info(self, datasets):
        """
        Query phedex "data" interface and fill the list of blocks.
        Argument is a list of datasets.
        """

        lock = threading.Lock()

        def inquire_phedex(list_chunk):
            options = [('level', 'block')]
            options.extend([('dataset', d.name) for d in list_chunk])

            source = self._make_phedex_request('data', options, method = POST)[0]['dataset']

            with lock:
                for ds_entry in source:
                    dataset = next(d for d in list_chunk if d.name == ds_entry['name'])
                    list_chunk.remove(dataset)
    
                    dataset.is_open = (ds_entry['is_open'] == 'y')
    
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
    
                        elif block.size != block_entry['bytes'] or block.num_files != block_entry['files'] or block.is_open != (block_entry['is_open'] == 'y'):
                            dataset.update_block(block_name, block_entry['bytes'], block_entry['files'], (block_entry['is_open'] == 'y'))

                        if block_entry['time_update'] > dataset.last_update:
                            dataset.last_update = block_entry['time_update']
    
                for dataset in list_chunk: # what remains - in case PhEDEx does not say anything about this dataset
                    dataset.blocks = []
                
        # set_constituent can take 10000 datasets at once, make it smaller and more parallel
        chunk_size = 100
        dataset_chunks = []

        start = 0
        while start < len(datasets):
            dataset_chunks.append(datasets[start:start + chunk_size])
            start += chunk_size

        parallel_exec(inquire_phedex, dataset_chunks, num_threads = 64)

    def _set_dataset_status_and_type(self, datasets):
        """
        Use DBS 'datasetlist' to set dataset status and type.
        Called by fill_dataset_info to inquire about production/unknown datasets,
        or by set_dataset_details for a full scan.
        Argument is a list of datasets.
        """

        def inquire_dbs(dataset_list):
            names = [d.name for d in dataset_list]
    
            dbs_entries = self._make_dbs_request('datasetlist', {'dataset': names, 'detail': True}, method = POST, format = 'json')
    
            for dataset in dataset_list:
                try:
                    ie = 0
                    while ie != len(dbs_entries):
                        if dbs_entries[ie]['dataset'] == dataset.name:
                            dbs_entry = dbs_entries.pop(ie)
                            break
                        ie += 1
                    else:
                        raise StopIteration()

                except StopIteration:
                    logger.info('set_dataset_details  Status of %s is unknown.', dataset.name)
                    dataset.status = Dataset.STAT_UNKNOWN
                    dataset.data_type = Dataset.TYPE_UNKNOWN
                    continue
    
                dataset.status = Dataset.status_val(dbs_entry['dataset_access_type'])
                dataset.data_type = Dataset.data_type_val(dbs_entry['primary_ds_type'])
                if dbs_entry['last_modification_date'] > dataset.last_update:
                    # normally last_update is determined by the last block update
                    # in case there was a change in the dataset info itself in DBS
                    dataset.last_update = dbs_entry['last_modification_date']

        # set_status_type can work on up to 1000 datasets
        chunk_size = 1000
        dataset_chunks = []

        start = 0
        while start < len(datasets):
            dataset_chunks.append(datasets[start:start + chunk_size])
            start += chunk_size

        parallel_exec(inquire_dbs, dataset_chunks)

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

        parallel_exec(inquire_dbs, datasets)

    def _make_phedex_request(self, resource, options = [], method = GET, format = 'url', raw_output = False):
        """
        Make a single PhEDEx request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._phedex_interface.make_request(resource, options = options, method = method, format = format)

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

        if not re.match('T[0-3]_.*', args.options[0]):
            print 'Arguments: site [group] dataset[#block] [dataset[#block] ..]comment'
            sys.exit(1)

        iopt = 0

        site = Site(args.options[iopt])
        iopt += 1

        if not re.match('/[^/]+/[^/]+/[^/]+', args.options[iopt]):
            group = Group(args.options[iopt])
            iopt += 1
        else:
            group = Group('AnalysisOps')

        replicas = []
        while True:
            if iopt == len(args.options):
                print 'Arguments: site [group] dataset[#block] [dataset[#block] ..] comment'
                sys.exit(1)

            if not re.match('/[^/]+/[^/]+/[^/]+', args.options[iopt]):
                break

            obj_name = args.options[iopt]
            iopt += 1
            
            if '#' in obj_name:
                dataset_name, block_name = obj_name.split('#')
            else:
                dataset_name = obj_name

            try:
                dataset_replica = next(replica for replica in replicas if replica.dataset.name == dataset_name)
            except StopIteration:
                dataset = Dataset(dataset_name)
                dataset_replica = DatasetReplica(dataset, site)
                replicas.append(dataset_replica)

            if '#' in obj_name:
                block = Block(Block.translate_name(block_name), dataset, 0, 0, False)
                # don't add the block to dataset (otherwise will become a dataset-level operation)
                block_replica = BlockReplica(block, site, group, True, False, 0)
                dataset_replica.block_replicas.append(block_replica)

        comments = ' '.join(args.options[iopt:])

        if command == 'delete':
            interface.schedule_deletions(replicas, comments = comments, is_test = args.is_test)

        elif command == 'subscribe':
            interface.schedule_copies(replicas, group, comments = comments, is_test = args.is_test)

        sys.exit(0)

    elif command == 'updaterequest' or command == 'updatesubscription':
        method = POST

    pprint.pprint(interface._make_phedex_request(command, options, method = method, raw_output = args.raw_output))
