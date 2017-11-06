import logging

from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.interface.webservice import RESTService, GET, POST
from common.dataformat import Dataset, Block
import common.configuration as config

logger = logging.getLogger(__name__)

class DBS(DatasetInfoSourceInterface):
    """
    Interface to DBS using DBSReader REST API.
    """
    
    def __init__(self):
        DatasetInfoSourceInterface.__init__(self)

        self._interface = RESTService(config.dbs.url_base)

    def set_dataset_details(self, datasets): #override
        first = 0
        while first < len(datasets):
            # fetch data 1000 at a time
            chunk_map = dict((d.name, d) for d in datasets[first:first + 1000])
            first += 1000
            
            ds_records = self._make_request('datasetlist', {'dataset': chunk_map.keys(), 'detail': True}, method = POST, format = 'json')            

            # This is still way too slow - have to make one API call (O(1)s) for each dataset.
            for ds_record in ds_records:
                dataset = chunk_map[ds_record['dataset']]
                dataset.status = Dataset.status_val(ds_record['dataset_access_type'])
                dataset.data_type = Dataset.data_type_val(ds_record['primary_ds_type'])
                dataset.last_update = ds_record['last_modification_date']
                dataset.blocks = []

                block_records = self._make_request('blocksummaries', ['dataset=' + ds_record['dataset'], 'detail=True'])
                for block_record in block_records:
                    block_name = Block.translate_name(block_record['block_name'].replace(dataset.name + '#', ''))

                    if block_record['open_for_writing'] == 1:
                        is_open = True
                        dataset.is_open = True
                    else:
                        is_open = False

                    block = dataset.find_block(block_name)

                    if block is None:
                        block = Block(block_name, dataset = dataset, size = block_record['block_size'], num_files = block_record['file_count'], is_open = is_open)
                
                        dataset.blocks.append(block)
                        dataset.size += block.size
                        dataset.num_files += block.num_files

                    else:
                        block.size = block_record['block_size']
                        block.num_files = block_record['file_count']
                        block.is_open = is_open

    def _make_request(self, resource, options = [], method = GET, format = 'url'):
        """
        Make a single DBS request call. Returns a list of dictionaries.
        """

        return self._interface.make_request(resource, options = options, method = method, format = format)


if __name__ == '__main__':

    from argparse import ArgumentParser
    import pprint

    parser = ArgumentParser(description = 'DBS Interface')

    parser.add_argument('command', metavar = 'COMMAND', help = 'Command to execute.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '*', default = [], help = 'Option string as passed to PhEDEx datasvc.')

    args = parser.parse_args()
    
    command = args.command

    interface = DBS()

    method = GET
    format = 'url'

    if command == 'datasetlist':
        # options: dataset=/A1/B1/C1,/A2/B2/C2,...
        options = {}
        for opt in args.options:
            key, eq, values = opt.partition('=')
            if key == 'dataset':
                datasets = values.split(',')
                options['dataset'] = datasets
            else:
                if values == 'True' or values == 'False':
                    value = bool(values)
                else:
                    try:
                        value = int(values)
                    except ValueError:
                        value = values

                options[key] = value

        method = POST
        format = 'json'

    else:
        options = args.options

    pprint.pprint(interface._make_request(command, options, method = method, format = format))
