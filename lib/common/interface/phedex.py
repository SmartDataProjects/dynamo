import os
import urllib
import urllib2
import httplib
import json
import logging
import collections

from common.interface.transfer import TransferInterface
from common.interface.statusprobe import StatusProbeInterface
from common.dataformat import Dataset, Block, Site, Group, DatasetReplica, BlockReplica
import common.configuration as config

logger = logging.getLogger(__name__)

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    _HTTPSGridAuthHandler_

    Get  proxy to acces PhEDEx API

    Needed for subscribe and delete calls

    Class variables:
    key  -- User key to CERN with access to PhEDEx
    cert -- User certificate connected to key
    """

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = config.phedex.x509_key
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.create_connection, req)

    def create_connection(self, host, timeout = 300):
        return httplib.HTTPSConnection(host, key_file = self.key, cert_file = self.cert)


def recursive_unicode_to_str(container):
    """
    Recursively convert unicode values in a nested container to strings.
    """

    if type(container) is list:
        for idx in range(len(container)):
            elem = container[idx]

            if type(elem) is unicode:
                container[idx] = str(elem)

            elif type(elem) is dict or type(elem) is list:
                recursive_unicode_to_str(elem)

    elif type(container) is dict:
        for key in container:
            elem = container[key]

            if type(elem) is unicode:
                container[key] = str(elem)

            elif type(elem) is dict or type(elem) is list:
                recursive_unicode_to_str(elem)



class PhEDExInterface(TransferInterface, StatusProbeInterface):
    """
    Interface to PhEDEx. Is a transfer and status probe interface at the same time.
    """

    ProtoBlockReplica = collections.namedtuple('ProtoBlockReplica', ['site_name', 'group_name', 'is_custodial', 'time_created', 'time_updated'])

    def __init__(self):
        self.url_base = config.phedex.url_base
        self._opener = urllib2.build_opener(HTTPSGridAuthHandler())

        self._last_request_time = 0
        self._last_request_url = ''

        # Due to the way PhEDEx is set up, we are required to see block replica information
        # when fetching the list of datasets. Might as well cache it.
        # Cache organized as {dataset: {block: replicas}}
        self._block_replicas = {}

    def get_site_list(self, filt = ''): #override
        option = ''
        if type(filt) is str and len(filt) != 0:
            option = 'node=' + filt
        elif type(filt) is list:
            option = '&' + '&'.join(['node=%s' % s for s in filt])

        source = self._make_request('nodes', option)

        site_list = {}

        for entry in source:
            name = entry['name']
            
            site_list[name] = Site(name, host = entry['se'], storage_type = Site.storage_type(entry['kind']), backend = entry['technology'])

        return site_list

    def get_group_list(self, filt = ''): #override
        if filt != '':
            option = 'group=' + filt
        else:
            option = ''

        source = self._make_request('groups', option)

        group_list = {}
        
        for entry in source:
            name = entry['name']

            group_list[name] = Group(name)

        return group_list

    def get_dataset_list(self, filt = '/*/*/*', site_filt = ''): #override
        self._block_replicas = {}

        sites = ''
        if type(site_filt) is str and len(site_filt):
            sites = '&node=' + site_filt
        elif type(site_filt) is list:
            sites = '&' + '&'.join(['node=%s' % s for s in site_filt])

        source = self._make_request('blockreplicas', 'subscribed=y&show_dataset=y&dataset=' + filt + sites)

        dataset_list = {}

        for dataset_entry in source:
            name = dataset_entry['name']

            dataset = Dataset(name, is_open = (dataset_entry['is_open'] == 'y'))

            self._block_replicas[dataset] = {}

            size_total = 0
            num_files_total = 0

            for block_entry in dataset_entry['block']:
                block_name = block_entry['name'].replace(name + '#', '')

                block = Block(block_name, dataset = dataset, size = block_entry['bytes'], num_files = block_entry['files'], is_open = (block_entry['is_open'] == 'y'))
                
                dataset.blocks.append(block)

                size_total += block_entry['bytes']
                num_files_total += block_entry['files']

                self._block_replicas[dataset][block] = []

                for replica_entry in block_entry['replica']:
                    replica = PhEDExInterface.ProtoBlockReplica(
                        site_name = replica_entry['node'],
                        group_name = replica_entry['group'],
                        is_custodial = (replica_entry['custodial'] == 'y'),
                        time_created = replica_entry['time_create'],
                        time_updated = replica_entry['time_update']
                    )

                    self._block_replicas[dataset][block].append(replica)

            dataset.size = size_total
            dataset.num_files = num_files_total

            dataset_list[name] = dataset

        return dataset_list
    
    def make_replica_links(self, sites, groups, datasets): #override
        # loop over datasets in memory and request block replica info
        for ds_name, dataset in datasets.items():
            custodial_sites = []
            num_blocks = {}

            for block, replicas in self._block_replicas[dataset].items():
                for proto_replica in replicas:
                    try:
                        site = sites[proto_replica.site_name]
                    except KeyError:
                        print 'Site', proto_replica.site_name, 'for replica of block', block.name, 'not registered.'
                        continue

                    if proto_replica.group_name is not None:
                        try:
                            group = groups[proto_replica.group_name]
                        except KeyError:
                            print 'Group', proto_replica.group_name, 'for replica of block', block.name, 'not registered.'
                            continue
                    else:
                        group = None

                    replica = BlockReplica(block, site, group = group, is_custodial = proto_replica.is_custodial, time_created = proto_replica.time_created, time_updated = proto_replica.time_updated)

                    block.replicas.append(replica)

                    if proto_replica.is_custodial and site not in custodial_sites:
                        custodial_sites.append(site)

                    try:
                        num_blocks[site] += 1
                    except KeyError:
                        num_blocks[site] = 1

            for site, num in num_blocks.items():
                replica = DatasetReplica(dataset, site, is_partial = (num != len(dataset.blocks)), is_custodial = (site in custodial_sites))

                dataset.replicas.append(replica)
            
    def _make_request(self, resource, option = ''):
        """
        Make a single PhEDEx request call.

        Return values:
        1 -- Status, 0 = everything went well, 1 = something went wrong
        2 -- IF status == 0 : HTTP response ELSE : Error message
        """

        url = self.url_base + '/' + resource
        if len(option) != 0:
            url += '?' + option

        logger.info(url)

        request = urllib2.Request(url)

        try:
            response = self._opener.open(request)

        except urllib2.HTTPError, e:
            raise

        except urllib2.URLError, e:
            raise

        result = json.loads(response.read())['phedex']

        logger.debug(result)

        self._last_request = result['request_timestamp']
        self._last_request_url = result['request_url']

        for metadata in ['request_timestamp', 'instance', 'request_url', 'request_version', 'request_call', 'call_time', 'request_date']:
            result.pop(metadata)
        
        # the only one item left in the results should be the result body
        result_body = result.values()[0]
        recursive_unicode_to_str(result_body)

        return result_body


if __name__ == '__main__':

    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'PhEDEx interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '?', default = '', help = 'Option string as passed to PhEDEx datasvc.')

    args = parser.parse_args()
    
    command = args.command

    interface = PhEDExInterface()

    print interface._make_request(command, args.options)
