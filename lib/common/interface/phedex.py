import json
import logging
import re
import collections

from common.interface.copy import CopyInterface
from common.interface.deletion import DeletionInterface
from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.replicainfo import ReplicaInfoSourceInterface
from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
from common.misc import unicode2str
import external.das.das_client as das_client
import common.configuration as config

logger = logging.getLogger(__name__)

class PhEDExDBSInterface(CopyInterface, DeletionInterface, SiteInfoSourceInterface, ReplicaInfoSourceInterface, DatasetInfoSourceInterface):
    """
    Interface to PhEDEx using datasvc REST API.
    """

    ProtoBlockReplica = collections.namedtuple('ProtoBlockReplica', ['block_name', 'group_name', 'is_custodial', 'is_complete', 'time_created', 'time_updated'])

    def __init__(self):
        self._phedex_interface = RESTService(config.phedex.url_base)
        self._dbs_interface = RESTService(config.dbs.url_base) # needed for detailed dataset info

        self._last_request_time = 0
        self._last_request_url = ''

        # Due to the way PhEDEx is set up, we are required to see block replica information
        # when fetching the list of datasets. Might as well cache it.
        # Cache organized as {site: {ds_name: [protoblocks]}}
        self._block_replicas = {}

    def schedule_copy(self, dataset, origin, dest, comments = ''): #override (CopyInterface)
        # origin argument is not used because of the way PhEDEx works

        request_body = self._get_file_catalog(dataset)

        options = {
            'node': dest.name,
            'data': request_body,
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

        if self.debug_mode:
            logger.debug('subscribe: %s', str(options))
            return

#        self._make_phedex_request('subscribe', options, method = POST)

    def schedule_deletion(self, replica, comments = ''): #override (DeletionInterface)
        if type(replica) == DatasetReplica:
            obj = replica.dataset

        elif type(replica) == BlockReplica:
            obj = replica.block

        request_body = self._get_file_catalog(obj)

        options = {
            'node': replica.site.name,
            'data': request_body,
            'level': 'dataset',
            'rm_subscriptions': 'y'
        }

        if comments:
            options['comments'] = comments

        if self.debug_mode:
            logger.debug('delete: %s', str(options))
            return

#        self._make_phedex_request('delete', options, method = POST)

    def get_site_list(self, sites, filt = '*'): #override (SiteInfoSourceInterface)
        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['node=' + filt]
        elif type(filt) is list:
            options = ['node=%s' % s for s in filt]

        source = self._make_phedex_request('nodes', options)

        for entry in source:
            if entry['name'] not in sites:
                site = Site(entry['name'], host = entry['se'], storage_type = Site.storage_type(entry['kind']), backend = entry['technology'])
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

        source = self._make_phedex_request('groups', options)
        
        for entry in source:
            if entry['name'] not in groups:
                group = Group(entry['name'])
                groups[entry['name']] = group

    def get_datasets_on_site(self, site, groups, filt = '/*/*/*'): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site.
        Cache block replica data to avoid making the call again.
        """

        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['dataset=' + filt]
        elif type(filt) is list:
            options = ['dataset=%s' % s for s in filt]

        self._block_replicas[site] = {}

        group_names = [g.name for g in groups]

        ds_name_list = []

        source = self._make_phedex_request('blockreplicas', ['subscribed=y', 'show_dataset=y', 'node=' + site.name] + options)

        logger.info('Got %d dataset info from site %s', len(source), site.name)

        for dataset_entry in source:
            ds_name = dataset_entry['name']

            block_replicas = []
            
            for block_entry in dataset_entry['block']:
                replica_entry = block_entry['replica'][0]

                if replica_entry['group'] not in group_names:
                    continue

                protoreplica = PhEDExInterface.ProtoBlockReplica(
                    block_name = block_entry['name'].replace(ds_name + '#', ''),
                    group_name = replica_entry['group'],
                    is_custodial = (replica_entry['custodial'] == 'y'),
                    is_complete = (replica_entry['complete'] == 'y'),
                    time_created = replica_entry['time_create'],
                    time_updated = replica_entry['time_update']
                )

                block_replicas.append(protoreplica)

            if len(block_replicas) != 0:
                ds_name_list.append(ds_name)
                self._block_replicas[site][ds_name] = block_replicas

        return ds_name_list
        
    def make_replica_links(self, datasets, sites, groups): #override (ReplicaInfoSourceInterface)
        # sites argument not used because cache is already site-aware

        for dataset in datasets.values():
            logger.info('Making replica links for dataset %s', dataset.name)
    
            custodial_sites = []
            replicas_on_site = {}
    
            for site, ds_block_list in self._block_replicas.items():
                if dataset.name not in ds_block_list:
                    continue
    
                for protoreplica in ds_block_list[dataset.name]:
                    try:
                        block = next(b for b in dataset.blocks if b.name == protoreplica.block_name)
                    except StopIteration:
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
                        is_custodial = protoreplica.is_custodial,
                        time_created = protoreplica.time_created,
                        time_updated = protoreplica.time_updated
                    )
    
                    block.replicas.append(replica)
    
                    if protoreplica.is_custodial and site not in custodial_sites:
                        custodial_sites.append(site)
    
                    try:
                        replicas_on_site[site].append(replica)
                    except KeyError:
                        replicas_on_site[site] = [replica]
    
            for site, block_replicas in replicas_on_site.items():
                replica = DatasetReplica(
                    dataset,
                    site,
                    is_complete = False,
                    is_partial = (len(block_replicas) != len(dataset.blocks)),
                    is_custodial = (site in custodial_sites)
                )

                try:
                    next(r for r in block_replicas if not r.is_complete)
                except StopIteration: # no incomplete block replicas
                    replica.is_complete = True
                
                replica.block_replicas = block_replicas
    
                dataset.replicas.append(replica)

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

        # is_open is True by default
        if dataset.is_open:
            source = self._make_phedex_request('data', ['level=block', 'dataset=' + name])[0]['dataset'] # PhEDEx returns a dictionary for each DBS instance
            ds_entry = source[0]

            self._set_dataset_constituent_info(dataset, ds_entry)

        # on_tape is False by default
        if not dataset.on_tape:
            source = self._make_phedex_request('blockreplicasummary', ['create_since=0', 'node=T*MSS', 'custodial=y', 'complete=y', 'dataset=' + name])
            blocks_on_tape = set(b['name'].replace(name + '#', '') for b in source)
            dataset_blocks = set(b.name for b in dataset.blocks)
        
            dataset.on_tape = (blocks_on_tape == dataset_blocks)

        if dataset.software_version[0] = 0:
            self._set_dataset_software_info(dataset)

        if dataset.data_type == Dataset.TYPE_UNKNOWN:
            self._set_dataset_type(dataset)

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

        options = [('level', 'block')]

        # Routine to fetch data and fill the datasets
        def run_datasets_query():
            if len(options) == 1:
                return

            source = self._make_phedex_request('data', options, method = POST)[0]['dataset']

            for ds_entry in source:
                self._set_dataset_constituent_info(datasets[ds_entry['name']], ds_entry)

            del options[1:] # keep only the first element (i.e. ('level', 'block'))

        # Loop over open datasets
        for dataset in datasets.values():
            if not dataset.is_open:
                continue

            options.append(('dataset', dataset.name))
            if len(options) >= 10001:
                run_datasets_query()

        run_datasets_query()

        # Use 'blockreplicasummary' query to check if all blocks of the dataset are on tape.
        # site=T*MSS -> tape

        options = [('create_since', '0'), ('node', 'T*MSS'), ('custodial', 'y'), ('complete', 'y')]
        blocks_on_tape = {}

        # Routine to fetch data and fill the list of blocks on tape
        def run_ontape_query():
            if len(options) == 4:
                return

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
            if dataset.on_tape:
                continue

            options.append(('dataset', dataset.name))

            if len(options) >= 10004:
                run_ontape_query()

        run_ontape_query()

        # Loop again and fill datasets
        for dataset in datasets.values():
            if dataset.on_tape:
                continue

            try:
                on_tape = set(blocks_on_tape[dataset.name])
            except KeyError:
                continue

            dataset_blocks = set(b.name for b in dataset.blocks)
            dataset.on_tape = (dataset_blocks == on_tape)

        # Loop over all datasets and fill other details if not set
        for dataset in datasets.values():
            if dataset.software_version[0] = 0:
                self._set_dataset_software_info(dataset)

            if dataset.data_type == Dataset.TYPE_UNKNOWN:
                self._set_dataset_type(dataset)

    def _set_dataset_constituent_info(self, dataset, ds_entry):
        ds_name = ds_entry['name']
        dataset.is_open = (ds_entry['is_open'] == 'y') # have seen cases where an obviously closed dataset is labeled open - need to check

        for block_entry in ds_entry['block']:
            block_name = block_entry['name'].replace(dataset.name + '#', '')

            if block_entry['is_open'] == 'y':
                is_open = True
            else:
                is_open = False

            block = Block(
                block_name,
                dataset = dataset,
                size = block_entry['bytes'],
                num_files = block_entry['files'],
                is_open = is_open
            )
        
            dataset.blocks.append(block)

        dataset.size = sum([b.size for b in dataset.blocks])
        dataset.num_files = sum([b.num_files for b in dataset.blocks])

    def _set_dataset_software_info(self, dataset):
        versions = self._make_dbs_request('releaseversions', ['dataset=' + dataset.name])[0]['release_version']

        # a dataset can have multiple versions; use the first one
        version = versions[0]

        matches = re.match('CMSSW_([0-9]+)_([0-9]+)_([0-9]+)(|_.*)', version)
        if matches:
            cycle, major, minor = map(int, [matches.group(i) for i in range(1, 4)])

            if matches.group(4):
                suffix = matches.group(4)[1:]
            else:
                suffix = ''

            dataset.software_version = (cycle, major, minor, suffix)

    def _set_dataset_type(self, dataset):
        dbs_entry = self._make_dbs_request('datasets', ['dataset=' + dataset.name, 'detail=True', 'dataset_access_type=VALID'])

        if len(dbs_entry) != 0:
            dataset.is_valid = True
        else:
            dataset.is_valid = False
            dbs_entry = self._make_dbs_request('datasets', ['dataset=' + dataset.name, 'detail=True', 'dataset_access_type=INVALID'])

            if len(dbs_entry) == 0:
                return

        if dbs_entry['primary_ds_type'] == 'data':
            dataset.data_type = Dataset.TYPE_DATA

        elif dbs_entry['primary_ds_type'] == 'mc':
            dataset.data_type = Dataset.TYPE_MC
            
    def _make_phedex_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single PhEDEx request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._phedex_interface.make_request(resource, options = options, method = method, format = format)
        logger.info('PhEDEx returned a response of ' + str(len(resp)) + ' bytes.')

        result = json.loads(resp)['phedex']
        unicode2str(result)

        logger.debug(result)

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

        logger.debug(result)

        return result

    def _get_file_catalog(self, obj):
        """
        Get the catalog of files for a given dataset / block. Used in subscribe() and delete().
        For delete() we might not have the full set of files at a given site - should we be
        querying for the replicas?
        """

        if type(obj) == Dataset:
            dataset = obj
            blocks = dataset.blocks

        elif type(obj) == Block:
            dataset = obj.dataset
            blocks = [obj]

        options = ['level=file', 'dataset=' + dataset.name]
        dataset_properties = {} # dict of properties
        block_properties = {} # block -> dict of properties
        files = {} # block -> list of files

        def run_data_query():
            ds_entry = self._make_request('data', options, method = POST)[0]['dataset'][0]
            if len(dataset_properties) == 0:
                for key, value in ds_entry.items():
                    if key == 'name' or key == 'block':
                        continue

                    dataset_properties[key.replace('_', '-')] = value # _ -> - for some reason..

            for block_entry in ds_entry['block']:
                block_name = block_entry['name'].replace(dataset.name + '#', '')
                for key, value in block_entry.items():
                    if key == 'name' or key == 'file':
                        continue

                    block_properties[block_name][key.replace('_', '-')] = value

                for file_entry in block_entry['file']:
                    files[block_name].append(tuple([file_entry[k] for k in ['lfn', 'size', 'checksum']]))

            del options[2:] # delete block names

        for block in blocks:
            options.append('block=' + dataset.name + '#' + block.name)
            block_properties[block.name] = {}
            files[block.name] = []
#            if sum(map(len, options)) + len(options) - 1 >= 7600 or block == blocks[-1]:
            if block == blocks[-1]:
                run_data_query()

        # we should consider using an actual xml tool
        xmlbase = '<data version="2.0"><dbs name="%s">{body}</dbs></data>' % config.dbs.url_base

        body = '<dataset name="%s"' % dataset.name
        for key, value in dataset_properties.items():
            body += ' %s="%s"' % (key, value)
        body += '>'

        for block in blocks:
            body += '<block name="%s#%s"' % (dataset.name, block.name)
            for key, value in block_properties[block.name].items():
                body += ' %s="%s"' % (key, value)
            body += '>'

            for lfn, size, checksum in files[block.name]:
                body += '<file name="%s" bytes="%d" checksum="%s"/>' % (lfn, size, checksum)
            
            body += '</block>'

        body += '</dataset>'

        xml = xmlbase.format(body = body)

        return xml


if __name__ == '__main__':

    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'PhEDEx interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '+', default = [], help = 'Option string as passed to PhEDEx datasvc.')

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    
    command = args.command

    interface = PhEDExInterface()

    print interface._make_request(command, args.options)
