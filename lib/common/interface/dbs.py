import json
import logging

from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService
from common.dataformat import Dataset, Block
from common.misc import unicode2str
import common.configuration as config

logger = logging.getLogger(__name__)

class DBSInterface(DatasetInfoSourceInterface):
    """
    Interface to DBS using DBSReader REST API.
    """
    
    def __init__(self):
        self._interface = RESTService(config.dbs.url_base)

    def get_dataset(self, name): # override
        # no need to query the DBS for the information on the dataset itself for the moment (we might need to if we add more data to Dataset class)
        dataset = Dataset(name)

        blocks = self._make_request('blocks', ['dataset=' + name, 'detail=True'])

        for entry in blocks:
            block_name = entry['block_name'].replace(dataset.name + '#', '')

            if entry['open_for_writing'] == 1:
                is_open = True
                dataset.is_open = True
            else:
                is_open = False

            block = Block(block_name, dataset = dataset, size = entry['block_size'], num_files = entry['file_count'], is_open = is_open)
        
            dataset.blocks.append(block)

        dataset.size = sum([b.size for b in dataset.blocks])
        dataset.num_files = sum([b.num_files for b in dataset.blocks])

        return dataset

    def _make_request(self, resource, options = []):
        """
        Make a single DBS request call. Returns a list of dictionaries.
        """

        resp = self._interface.make_request(resource, options)
        logger.info('DBS returned a response of ' + str(len(resp)) + ' bytes.')

        result = json.loads(resp)
        logger.debug(result)

        unicode2str(result)

        return result


if __name__ == '__main__':

    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'DBS Interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '+', default = [], help = 'Option string as passed to PhEDEx datasvc.')

    args = parser.parse_args()
    
    logger.setLevel(logging.DEBUG)
    
    command = args.command

    interface = DBSInterface()

    print interface._make_request(command, args.options)
