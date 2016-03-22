import json
import logging
import collections

from common.interface.copy import CopyInterface
from common.interface.deletion import DeletionInterface
from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.replicainfo import ReplicaInfoSourceInterface
from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
from common.misc import unicode2str
import common.configuration as config

logger = logging.getLogger(__name__)

class PhEDExInterface(CopyInterface, DeletionInterface, SiteInfoSourceInterface, ReplicaInfoSourceInterface, DatasetInfoSourceInterface):
    """
    Interface to PhEDEx using datasvc REST API.
    """

    ProtoBlockReplica = collections.namedtuple('ProtoBlockReplica', ['block_name', 'group_name', 'is_custodial', 'time_created', 'time_updated'])

    def __init__(self):
        self._interface = RESTService(config.phedex.url_base)

        self._last_request_time = 0
        self._last_request_url = ''

        # Due to the way PhEDEx is set up, we are required to see block replica information
        # when fetching the list of datasets. Might as well cache it.
        # Cache organized as {site: {ds_name: [protoblocks]}}
        self._block_replicas = {}

    def schedule_copy(self, dataset, origin, dest): #override (CopyInterface)
        print 'Copy', dataset.name, 'from', origin.name, 'to', dest.name

    def schedule_deletion(self, obj, site): #override (DeletionInterface)
        print 'Delete', obj.name, 'from', site.name

    def get_site_list(self, filt = '*'): #override (SiteInfoSourceInterface)
        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['node=' + filt]
        elif type(filt) is list:
            options = ['node=%s' % s for s in filt]

        source = self._make_request('nodes', options)

        site_list = []

        for entry in source:
            site_list.append(Site(entry['name'], host = entry['se'], storage_type = Site.storage_type(entry['kind']), backend = entry['technology']))

        return site_list

    def get_group_list(self, filt = '*'): #override (SiteInfoSourceInterface)
        options = []
        if type(filt) is str and len(filt) != 0:
            options = ['group=' + filt]
        elif type(filt) is list:
            options = ['group=%s' % s for s in filt]

        source = self._make_request('groups', options)

        group_list = []
        
        for entry in source:
            group_list.append(Group(entry['name']))

        return group_list

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

        source = self._make_request('blockreplicas', ['subscribed=y', 'show_dataset=y', 'node=' + site.name] + options)

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

        for dataset in datasets:
            logger.info('Making replica links for dataset %s', dataset.name)
    
            custodial_sites = []
            num_blocks = {}
    
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
                            continue
                    else:
                        group = None
                    
                    replica = BlockReplica(block, site, group = group, is_custodial = protoreplica.is_custodial, time_created = protoreplica.time_created, time_updated = protoreplica.time_updated)
    
                    block.replicas.append(replica)
    
                    if protoreplica.is_custodial and site not in custodial_sites:
                        custodial_sites.append(site)
    
                    try:
                        num_blocks[site] += 1
                    except KeyError:
                        num_blocks[site] = 1
    
            for site, num in num_blocks.items():
                replica = DatasetReplica(dataset, site, is_partial = (num != len(dataset.blocks)), is_custodial = (site in custodial_sites))
    
                dataset.replicas.append(replica)

    def get_dataset(self, name): #override (DatasetInfoSourceInterface)
        source = self._make_request('data', ['level=block', 'dataset=' + name])[0]['dataset'] # PhEDEx returns a dictionary for each DBS instance
        ds_entry = source[0]

        dataset = self._construct_dataset(source[0])

        source = self._make_request('blockreplicasummary', ['create_since=0', 'node=T*MSS', 'custodial=y', 'complete=y', 'dataset=' + name])
        blocks_on_tape = set(b['name'].replace(name + '#', '') for b in source)
        dataset_blocks = set(b.name for b in dataset.blocks)
        
        dataset.on_tape = (blocks_on_tape == dataset_blocks)

    def get_datasets(self, names): #override (DatasetInfoSourceInterface)
        """
        Reduce the number of queries made for more efficient data processing. Called by
        InventoryManager.
        """

        datasets = []

        # accumulate dataset=/A/B/C options and make a query once the length of the
        # GET command reaches a threshold
        options = ['level=block']

        def run_datasets_query():
            source = self._make_request('data', options)[0]['dataset']

            for ds_entry in source:
                datasets.append(self._construct_dataset(ds_entry))

            del options[1:] # keep only the first element (i.e. 'level=block')

        for ds_name in names:
            options.append('dataset=' + ds_name)
            # if the total string length (including &'s) is close to 8 kB or this is the last name
            if sum(map(len, options)) + len(options) - 1 >= 7600 or ds_name == names[-1]:
                run_datasets_query()

        options = ['create_since=0', 'node=T*MSS', 'custodial=y', 'complete=y']
        blocks_on_tape = {}

        def run_ontape_query():
            source = self._make_request('blockreplicasummary', options)

            for block_entry in source:
                name = block_entry['name']
                ds_name = name[:name.find('#')]
                block_name = name[name.find('#') + 1:]

                try:
                    blocks_on_tape[ds_name].append(block_name)
                except KeyError:
                    blocks_on_tape[ds_name] = [block_name]

            del options[4:] # delete dataset=/A/B/C&dataset=/D/E/F&...

        for ds_name in names:
            options.append('dataset=' + ds_name)
            # if the total string length (including &'s) is close to 8 kB or this is the last name
            if sum(map(len, options)) + len(options) - 1 >= 7600 or ds_name == names[-1]:
                run_ontape_query()

        for dataset in datasets:
            try:
                on_tape = set(blocks_on_tape[dataset.name])
            except KeyError:
                continue

            dataset.on_tape = (set(b.name for b in dataset.blocks) == on_tape)

        return datasets
            
    def _make_request(self, resource, options = []):
        """
        Make a single PhEDEx request call. Returns a list of dictionaries from the body of the query result.
        """

        resp = self._interface.make_request(resource, options)
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

    def _construct_dataset(self, ds_entry):
        ds_name = ds_entry['name']
        dataset = Dataset(ds_name)
        dataset.is_valid = True # PhEDEx does not know about dataset validity (perhaps all datasets it knows are valid)
        dataset.is_open = (ds_entry['is_open'] == 'y') # have seen cases where an obviously closed dataset is labeled open - need to check

        for block_entry in ds_entry['block']:
            block_name = block_entry['name'].replace(dataset.name + '#', '')

            if block_entry['is_open'] == 'y':
                is_open = True
            else:
                is_open = False

            block = Block(block_name, dataset = dataset, size = block_entry['bytes'], num_files = block_entry['files'], is_open = is_open)
        
            dataset.blocks.append(block)

        dataset.size = sum([b.size for b in dataset.blocks])
        dataset.num_files = sum([b.num_files for b in dataset.blocks])

        return dataset


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
