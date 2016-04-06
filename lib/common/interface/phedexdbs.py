import json
import logging
import time
import re
import collections
import pprint

from common.interface.copy import CopyInterface
from common.interface.deletion import DeletionInterface
from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.replicainfo import ReplicaInfoSourceInterface
from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
from common.misc import unicode2str
import common.configuration as config

logger = logging.getLogger(__name__)

ProtoBlockReplica = collections.namedtuple('ProtoBlockReplica', ['block_name', 'group_name', 'is_custodial', 'is_complete'])

FileInfo = collections.namedtuple('File', ['name', 'bytes', 'checksum'])

class PhEDExDBS(CopyInterface, DeletionInterface, SiteInfoSourceInterface, ReplicaInfoSourceInterface, DatasetInfoSourceInterface):
    """
    Interface to PhEDEx using datasvc REST API.
    """

    def __init__(self, phedex_url = config.phedex.url_base, dbs_url = config.dbs.url_base):
        CopyInterface.__init__(self)
        DeletionInterface.__init__(self)
        SiteInfoSourceInterface.__init__(self)
        ReplicaInfoSourceInterface.__init__(self)
        DatasetInfoSourceInterface.__init__(self)

        self._phedex_interface = RESTService(phedex_url)
        self._dbs_interface = RESTService(dbs_url) # needed for detailed dataset info

        self._last_request_time = 0
        self._last_request_url = ''

        # Due to the way PhEDEx is set up, we are required to see block replica information
        # when fetching the list of datasets. Might as well cache it.
        # Cache organized as {dataset: {site: [ProtoBlockReplicas]}}
        self._block_replicas = {}

    def schedule_copy(self, dataset, dest, origin = None, comments = ''): #override (CopyInterface)
        # origin argument is not used because of the way PhEDEx works

        catalogs = self._get_file_catalog(dataset)

        options = {
            'node': dest.name,
            'data': self._form_catalog_xml(catalogs),
            'level': 'dataset',
            'priority': 'low',
            'move': 'n',
            'static': 'n',
            'custodial': 'n',
            'group': 'AnalysisOps',
            'request_only': 'n',
            'no_mail': 'n'
        }

        if comments:
            options['comments'] = comments

        if config.read_only:
            logger.debug('schedule_copy  subscribe: %s', str(options))
            return

        self._make_phedex_request('subscribe', options, method = POST)

    def schedule_deletion(self, replica, comments = ''): #override (DeletionInterface)
        if type(replica) == DatasetReplica:
            dataset = replica.dataset

        elif type(replica) == BlockReplica:
            dataset = replica.block.dataset

        catalogs = self._get_file_catalog(dataset)

        options = {
            'node': replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': 'dataset',
            'rm_subscriptions': 'y'
        }

        if comments:
            options['comments'] = comments

        if config.read_only:
            logger.info('schedule_deletion  delete %d datasets', len(catalogs))
            logger.debug('schedule_deletion  delete: %s', str(options))
            return 0

        result = self._make_phedex_request('delete', options, method = POST)

        if len(result) == 0:
            logger.error('schedule_deletions  delete failed.')
            return 0

        request_id = int(result[0]['id']) # return value is a string

        logger.warning('PhEDEx deletion request id: %d', request_id)
        
        result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': replica.site.name}, method = POST)

        if len(result) == 0:
            logger.error('schedule_deletions  deletion approval failed.')
            return 0

        return request_id

    def schedule_deletions(self, replica_list, comments = ''): #override (DeletionInterface)

        all_datasets = list(set([r.dataset for r in replica_list]))
        all_catalogs = self._get_file_catalog(all_datasets)

        request_mapping = {}

        def run_deletion_request(site, replicas_to_delete):
            catalogs = dict([(r.dataset, all_catalogs[r.dataset]) for r in replicas_to_delete])

            options = {
                'node': site.name,
                'data': self._form_catalog_xml(catalogs),
                'level': 'dataset',
                'rm_subscriptions': 'y'
            }

            if comments:
                options['comments'] = comments

            if config.read_only:
                logger.debug('schedule_deletions  delete: %s', str(options))
                return

            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
            result = self._make_phedex_request('delete', options, method = POST)

            if len(result) == 0:
                logger.error('schedule_deletions  delete failed.')
                return

            request_id = int(result[0]['id']) # return value is a string

            logger.warning('PhEDEx deletion request id: %d', request_id)
            
            request_mapping[request_id] = replicas_to_delete

            result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)

            if len(result) == 0:
                logger.error('schedule_deletions  deletion approval failed.')
                return

        replicas_by_site = {}
        for replica in replica_list:
            try:
                replicas_by_site[replica.site].append(replica)
            except KeyError:
                replicas_by_site[replica.site] = [replica]

        for site, replica_list in replicas_by_site.items():
            deletion_chunk = []
            chunk_size = 0
            for replica in replica_list:
                deletion_chunk.append(replica)
                chunk_size += replica.size()
                if chunk_size >= config.dbs.deletion_chunk_size or replica == replica_list[-1]:
                    run_deletion_request(site, deletion_chunk)
                    deletion_chunk = []
                    chunk_size = 0

        return request_mapping

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
                # temporary
                site.capacity = 10000
                site.used_total = 9000
                # temporary
                sites[entry['name']] = site

    def get_group_list(self, groups, filt = '*'): #override (SiteInfoSourceInterface)
        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['group=' + filt]
        elif type(filt) is list:
            options = ['group=%s' % s for s in filt]

        logger.info('get_group_list  Fetching the list of groups from PhEDEx')
        source = self._make_phedex_request('groups', options)
        
        for entry in source:
            if entry['name'] not in groups:
                group = Group(entry['name'])
                groups[entry['name']] = group

    def get_dataset_names(self, sites = None, groups = None, filt = '/*/*/*'): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site.
        Cache block replica data to avoid making the call again.
        """

        options = ['subscribed=y', 'show_dataset=y']
        if sites is not None:
            for site in sites:
                options.append('node=' + site.name)

        if type(filt) is str and len(filt) != 0:
            options += ['dataset=' + filt]
        elif type(filt) is list:
            options += ['dataset=%s' % s for s in filt]

        ds_name_list = []

        source = self._make_phedex_request('blockreplicas', options)

        logger.info('get_dataset_names  Got %d dataset info', len(source))

        for dataset_entry in source:
            ds_name = dataset_entry['name']

            if ds_name not in self._block_replicas:
                self._block_replicas[ds_name] = {}

            has_block_replica = False

            for block_entry in dataset_entry['block']:
                block_name = block_entry['name'].replace(ds_name + '#', '')

                for replica_entry in block_entry['replica']:
                    if groups is not None and replica_entry['group'] not in groups:
                        continue

                    has_block_replica = True

                    site_name = replica_entry['node']

                    protoreplica = ProtoBlockReplica(
                        block_name = block_name,
                        group_name = replica_entry['group'],
                        is_custodial = (replica_entry['custodial'] == 'y'),
                        is_complete = (replica_entry['complete'] == 'y')
                    )

                    if site_name not in self._block_replicas[ds_name]:
                        self._block_replicas[ds_name][site_name] = []
    
                    self._block_replicas[ds_name][site_name].append(protoreplica)
                    
            if has_block_replica:
                ds_name_list.append(ds_name)

        return ds_name_list
        
    def make_replica_links(self, sites, groups, datasets): #override (ReplicaInfoSourceInterface)
        """
        Loop over datasets and protoreplicas for the dataset.
        Make a block replica out of each protoreplica.
        Make a dataset replica for each dataset-site combination.
        """

        for dataset in datasets.values():
            logger.info('make_replica_links  Making replica links for dataset %s', dataset.name)
    
            for site_name, ds_block_list in self._block_replicas[dataset.name].items():
                site = sites[site_name]
    
                for protoreplica in ds_block_list:
                    try:
                        block = next(b for b in dataset.blocks if b.name == protoreplica.block_name)
                    except StopIteration:
                        if dataset.status == Dataset.STAT_VALID:
                            logger.warning('Replica interface found a block %s that is unknown to dataset %s', protoreplica.block_name, dataset.name)

                        continue
    
                    if protoreplica.group_name is not None:
                        try:
                            group = groups[protoreplica.group_name]
                        except KeyError:
                            logger.warning('Group %s for replica of block %s not registered.', protoreplica.group_name, block.name)
                            group = None
                    else:
                        group = None
                    
                    replica = BlockReplica(
                        block,
                        site,
                        group = group,
                        is_complete = protoreplica.is_complete,
                        is_custodial = protoreplica.is_custodial
                    )
    
                    block.replicas.append(replica)
                    site.block_replicas.append(replica)

                    # now find the dataset replica
                    dataset_replica = dataset.find_replica(site)
                    if dataset_replica is None:
                        # make one if not made yet
                        dataset_replica = DatasetReplica(
                            dataset,
                            site,
                            group = group,
                            is_complete = True,
                            is_partial = False,
                            is_custodial = False
                        )

                        dataset.replicas.append(dataset_replica)
                        site.dataset_replicas.append(dataset_replica)

                    # add the block replica to the list
                    dataset_replica.block_replicas.append(replica)

                    # if any block replica is not complete, dataset replica is not
                    if not replica.is_complete:
                        dataset_replica.is_complete = False

                    # if any of the block replica is custodial, dataset replica is also
                    if replica.is_custodial:
                        dataset_replica.is_custodial = True

                    # if any of the block replica has a different owner, dataset replica owner is None
                    if replica.group != dataset_replica.group:
                        dataset_replica.group = None

            for replica in dataset.replicas:
                replica.is_partial = (len(replica.block_replicas) != len(dataset.blocks))
    
            # remove cache to save memory
            self._block_replicas.pop(dataset.name)

    def get_dataset(self, name, datasets): #override (DatasetInfoSourceInterface)
        """
        If name is found in the list and the dataset is not open and is on tape, return.
        Otherwise fetch information.
        """

        if name in datasets:
            dataset = datasets[name]
        else:
            dataset = Dataset(name)
            datasets[name] = dataset

        if dataset.status == Dataset.STAT_IGNORED:
            return

        logger.info('get_dataset  Fetching data for %s', name)

        self.set_dataset_constituent_info([dataset])

        if dataset.software_version[0] == 0:
            self._set_dataset_software_info(dataset)

        if dataset.data_type == Dataset.TYPE_UNKNOWN:
            self.set_dataset_details([dataset])

    def get_datasets(self, names, datasets): #override (DatasetInfoSourceInterface)
        """
        Reduce the number of queries made for more efficient data processing. Called by
        InventoryManager.
        """

        for name in names:
            if name not in datasets:
                datasets[name] = Dataset(name)

        # Using POST requests with PhEDEx:
        # Accumulate dataset=/A/B/C options and make a query once every 10000 entries
        # PhEDEx does not document a hard limit on the length of POST request list.
        # 10000 was experimentally verified to be OK.

        # Use 'data' query for full lists of blocks (Possibly 'blockreplicas' already
        # has this information, to be verified) for open datasets.

        production_datasets = [d for d in datasets.values() if d.status == Dataset.STAT_PRODUCTION or d.status == Dataset.STAT_UNKNOWN]

        self.set_dataset_constituent_info(production_datasets)

        for dataset in production_datasets:
            if len(dataset.blocks) == 0:
                logger.info('get_datasets::run_datasets_query  %s does not have any blocks and is removed.', dataset.name)
                datasets.pop(dataset.name)

        # Loop over all datasets and fill other details if not set
        for dataset in datasets.values():
            if dataset.status == Dataset.STAT_IGNORED:
                continue

            if dataset.software_version[0] == 0:
                self._set_dataset_software_info(dataset)

            if dataset.data_type == Dataset.TYPE_UNKNOWN:
                self.set_dataset_details([dataset])

    def find_tape_copies(self, datasets): #override (ReplicaInfoSourceInterface)
        # Use 'blockreplicasummary' query to check if all blocks of the dataset are on tape.
        # site=T*MSS -> tape

        options = [('create_since', '0'), ('node', 'T*MSS'), ('custodial', 'y'), ('complete', 'y')]
        blocks_on_tape = {}

        # Routine to fetch data and fill the list of blocks on tape
        def run_ontape_query():
            if len(options) == 4:
                return

            logger.info('find_tape_copies::run_ontape_query  Checking whether %d datasets (%s, ...) are on tape', len(options) - 4, options[4][1])
            source = self._make_phedex_request('blockreplicasummary', options, method = POST)

            for block_entry in source:
                name = block_entry['name']
                ds_name = name[:name.find('#')]
                block_name = name[name.find('#') + 1:]

                try:
                    blocks_on_tape[ds_name].append(block_name)
                except KeyError:
                    blocks_on_tape[ds_name] = [block_name]

            del options[4:] # delete dataset specifications

        # Loop over datasets not on tape
        for dataset in datasets.values():
            # on_tape is False by default
            if dataset.on_tape or dataset.status == Dataset.STAT_IGNORED:
                continue

            options.append(('dataset', dataset.name))

            if len(options) >= 10004:
                run_ontape_query()

        run_ontape_query()

        # Loop again and fill datasets
        for dataset in datasets.values():
            if dataset.on_tape or dataset.status == Dataset.STAT_IGNORED:
                continue

            try:
                on_tape = set(blocks_on_tape[dataset.name])
            except KeyError:
                continue

            dataset_blocks = set(b.name for b in dataset.blocks)
            dataset.on_tape = (dataset_blocks == on_tape)

    def set_dataset_constituent_info(self, datasets): #override (DatasetInfoSourceInterface)
        start = 0
        while start < len(datasets):
            list_chunk = datasets[start:start + 10000]

            start += 10000

            options = [('level', 'block')]
            options += [('dataset', d.name) for d in list_chunk]

            logger.info('get_datasets::run_datasets_query  Fetching data for %d datasets.', len(options) - 1)
            source = self._make_phedex_request('data', options, method = POST)[0]['dataset']
    
            for ds_entry in source:
                dataset = next(d for d in list_chunk if d.name == ds_entry['name'])

                dataset.is_open = (ds_entry['is_open'] == 'y') # useless flag - all datasets are flagged open
        
                has_open_blocks = False
        
                for block_entry in ds_entry['block']:
                    block_name = block_entry['name'].replace(dataset.name + '#', '')

                    block = dataset.find_block(block_name)

                    if block is None:
                        block = Block(
                            block_name,
                            dataset = dataset,
                            size = block_entry['bytes'],
                            num_files = block_entry['files'],
                            is_open = False
                        )
                        dataset.blocks.append(block)
        
                    if block_entry['is_open'] == 'y' and time.time() - block_entry['time_create'] > 48. * 3600.:
                        # Block is more than 48 hours old and is still open - PhEDEx can be wrong
                        logger.info('set_dataset_constituent_info  Double-checking with DBS if block %s#%s is open', dataset.name, block_name)
                        dbs_result = self._make_dbs_request('blocks', ['block_name=' + dataset.name + '%23' + block_name, 'detail=True']) # %23 = '#'
                        if len(dbs_result) == 0 or dbs_result[0]['open_for_writing'] == 1:
                            # cannot get data from DBS, or DBS also says this block is open
                            block.is_open = True
                            has_open_blocks = True
        
                dataset.size = sum([b.size for b in dataset.blocks])
                dataset.num_files = sum([b.num_files for b in dataset.blocks])
        
                # TODO this is not fully accurate
                if has_open_blocks:
                    dataset.status = Dataset.STAT_PRODUCTION
                else:
                    dataset.status = Dataset.STAT_VALID

    def _set_dataset_software_info(self, dataset):
        logger.info('set_dataset_software_info  Fetching software version for %s', dataset.name)
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

    def set_dataset_details(self, datasets): #override (DatasetInfoSourceInterface)
        logger.info('set_dataset_deatils  Checking status of %d datasets', len(datasets))

        start = 0
        while start < len(datasets):
            dataset_list = datasets[start:start + 1000]
            names = [d.name for d in dataset_list]
    
            dbs_entries = self._make_dbs_request('datasetlist', {'dataset': names, 'detail': True}, method = POST, format = 'json')
    
            for dataset in dataset_list:
                try:
                    dbs_entry = next(e for e in dbs_entries if e['dataset'] == dataset.name)
                except StopIteration:
                    logger.info('set_dataset_details  Status of %s is unknown.', dataset.name)
                    dataset.status = Dataset.STAT_UNKNOWN
                    continue
    
                dataset.status = Dataset.status_val(dbs_entry['dataset_access_type'])
                dataset.data_type = Dataset.data_type_val(dbs_entry['primary_ds_type'])
                dataset.last_update = dbs_entry['last_modification_date']

            start += 1000
            
    def _make_phedex_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single PhEDEx request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._phedex_interface.make_request(resource, options = options, method = method, format = format)
        logger.info('PhEDEx returned a response of ' + str(len(resp)) + ' bytes.')

        try:
            result = json.loads(resp)['phedex']
        except KeyError:
            logger.error(resp)
            return

        unicode2str(result)

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(pprint.pformat(result))

        self._last_request = result['request_timestamp']
        self._last_request_url = result['request_url']

        for metadata in ['request_timestamp', 'instance', 'request_url', 'request_version', 'request_call', 'call_time', 'request_date']:
            result.pop(metadata)
        
        # the only one item left in the results should be the result body
        return result.values()[0]

    def _make_dbs_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single DBS request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._dbs_interface.make_request(resource, options = options, method = method, format = format)
        logger.info('DBS returned a response of ' + str(len(resp)) + ' bytes.')

        result = json.loads(resp)
        unicode2str(result)

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(pprint.pformat(result))

        return result

    def _get_file_catalog(self, obj):
        """
        Get the catalog of files for a given dataset / block. Used in subscribe() and delete().
        For delete() we might not have the full set of files at a given site - should we be
        querying for the replicas?
        """

        if type(obj) == Dataset:
            datasets = {obj.name: obj}

        elif type(obj) == Block:
            datasets = {obj.dataset.name: obj.dataset}

        elif type(obj) == list:
            datasets = {}
            for elem in obj:
                if type(elem) == Dataset:
                    datasets[elem.name] = elem
                elif type(elem) == Block:
                    datasets[elem.dataset.name] = elem.dataset

        options = ['level=file']

        file_catalogs = {} # dataset -> block -> list of files

        def run_data_query():
            if len(options) == 1:
                return

            logger.info('Querying file data for %d datasets.', len(options) - 1)

            ds_entries = self._make_phedex_request('data', options, method = POST)[0]['dataset']

            for ds_entry in ds_entries:
                ds_name = ds_entry['name']
                dataset = datasets[ds_name]

                catalog = dict([(b, []) for b in dataset.blocks])
                file_catalogs[dataset] = catalog
                
                for block_entry in ds_entry['block']:
                    block_name = block_entry['name'].replace(ds_name + '#', '')
                    try:
                        block = next(b for b in dataset.blocks if b.name == block_name)
                    except StopIteration:
                        logger.warning('Unknown block %s found in %s', block_name, dataset.name)
                        continue
                    
                    for file_entry in block_entry['file']:
                        catalog[block].append(FileInfo(*tuple([file_entry[k] for k in ['lfn', 'size', 'checksum']])))

            del options[1:] # delete block names

        for dataset_name in datasets.keys():
            options.append('dataset=' + dataset_name)
            if len(options) >= 1001:
                run_data_query()

        run_data_query()

        return file_catalogs

    def _form_catalog_xml(self, file_catalogs, human_readable = False):

        # we should consider using an actual xml tool
        if human_readable:
            xml = '<data version="2.0">\n <dbs name="%s">\n' % config.dbs.url_base
        else:
            xml = '<data version="2.0"><dbs name="%s">' % config.dbs.url_base

        for dataset, catalogs in file_catalogs.items():
            if human_readable:
                xml += '  '

            xml += '<dataset name="%s" is-open="%s" is-transient="%s">' % (dataset.name, 'y' if dataset.is_open else 'n', 'n')

            if human_readable:
                xml += '\n'

            for block in dataset.blocks:
                if human_readable:
                    xml += '   '
                
                xml += '<block name="%s#%s" is-open="%s">' % (dataset.name, block.name, 'y' if block.is_open else 'n')

                if human_readable:
                    xml += '\n'

                for lfn, size, checksum in catalogs[block]:
                    if human_readable:
                        xml += '    '

                    xml += '<file name="%s" bytes="%d" checksum="%s"/>' % (lfn, size, checksum)

                    if human_readable:
                        xml += '\n'
                
                if human_readable:
                    xml += '   '

                xml += '</block>'

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

    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    command = args.command

    interface = PhEDExDBS(phedex_url = args.phedex_url)

    if args.method == 'POST':
        method = POST
    else:
        method = GET

    options = args.options

    if command == 'delete' or command == 'subscribe':
        method = POST

        if args.phedex_url == config.phedex.url_base or 'prod' in args.phedex_url:
            print 'Are you sure you want to run this command on a prod instance? [Y/n]'
            response = sys.stdin.readline().strip()
            if response != 'Y':
                sys.exit(0)

        if command == 'subscribe':
            options = {
                'node': args.options[0],
                'level': 'dataset',
                'priority': 'low',
                'move': 'n',
                'static': 'n',
                'custodial': 'n',
                'group': 'AnalysisOps',
                'request_only': 'n',
                'no_mail': 'n'
            }

        else:
            options = {
                'node': args.options[0],
                'level': 'dataset',
                'rm_subscriptions': 'y'
            }
            
        options['data'] = '<>'

    elif command == 'updaterequest':
        method = POST

    pprint.pprint(interface._make_phedex_request(command, options, method = method))
