import os
import urllib
import urllib2
import httplib
import json

from common.interface.transfer import TransferInterface
from common.interface.statusprobe import StatusProbeInterface
from common.dataformat import Dataset, Block, Site, DatasetReplica, BlockReplica
import common.configuration as config

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

    def __init__(self):
        self.url_base = config.phedex.url_base
        self._opener = urllib2.build_opener(HTTPSGridAuthHandler())

        self._last_request_time = 0
        self._last_request_url = ''

        self._block_replica_data = {}

    def get_site_list(self, name = ''): #override
        if name != '':
            option = 'node=' + name
        else:
            option = ''

        source = self._make_request('nodes', option)

        site_list = {}

        for datum in source:
            site_name = datum['name']
                
            site_list[site_name] = Site(site_name, host = datum['se'], storage_type = Site.storage_type(datum['kind']), backend = datum['technology'])

        return site_list

    def get_dataset_list(self, name = '/*/*/*'): #override
        self._block_replica_data = {}

        source = self._make_request('blockreplicas', 'show_dataset=y&dataset=' + name)

        dataset_list = {}

        for dataset_datum in source:
            dataset_name = dataset_datum['name']

            dataset = Dataset(dataset_name)

            size_total = 0
            num_files_total = 0

            for block_datum in dataset_datum['block']:
                block_name = block_datum['name'].replace(dataset_name + '#', '')

                block = Block(block_name, dataset = dataset, size = block_datum['bytes'], num_files = block_datum['files'], is_open = (block_datum['is_open'] == 'y'))
                
                dataset.blocks.append(block)

                replica_data = []
                for replica_datum in block_datum['replica']:
                    replica_data.append((replica_datum['node'], (replica_datum['custodial'] == 'y'), replica_datum['time_create'], replica_datum['time_update']))

                self._block_replica_data[block] = replica_data

                size_total += block_datum['bytes']
                num_files_total += block_datum['files']

            dataset.size = size_total
            dataset.num_files = num_files_total

            dataset_list[dataset_name] = dataset

        return dataset_list
    
    def make_replica_links(self, sites, datasets): #override
        for ds_name, dataset in datasets.items():
            custodial_sites = []
            num_blocks = {}

            for block in dataset.blocks:
                for replica_data in self._block_replica_data[block]:
                    site_name, custodial, time_created, time_updated = replica_data
                    site = sites[site_name]

                    replica = BlockReplica(block, site, is_custodial = custodial, time_created = time_created, time_updated = time_updated)

                    block.replicas.append(replica)

                    if custodial and site not in custodial_sites:
                        custodial_sites.append(site)

                    try:
                        num_blocks[site] += 1
                    except KeyError:
                        num_blocks[site] = 1

            for site, num in num_blocks.items():
                replica = DatasetReplica(dataset, site, is_partial = (num != len(dataset.blocks)))
                if site in custodial_sites:
                    replica.is_custodial = True

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

        if config.debug_level > 0:
            print url

        request = urllib2.Request(url)

        try:
            response = self._opener.open(request)

        except urllib2.HTTPError, e:
            raise

        except urllib2.URLError, e:
            raise

        result = json.loads(response.read())['phedex']

        if config.debug_level > 2:
            print result

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
