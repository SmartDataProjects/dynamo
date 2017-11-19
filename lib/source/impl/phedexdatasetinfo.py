"""
DatasetInfoSource using PhEDEx and DBS.
"""

import logging
import fnmatch
import re

from source.datasetinfo import DatasetInfoSource
from common.interface.phedex import PhEDEx
from common.interface.webservice import RESTService
from dataformat import Dataset, Block, File

LOG = logging.getLogger(__name__)

class PhEDExDatasetInfoSource(DatasetInfoSource):
    def __init__(self, config):
        DatasetInfoSource.__init__(self, config)

        self._phedex = PhEDEx()
        self._dbs = RESTService(config.dbs_url)

    def get_dataset(self, name):
        ## Get the full dataset-block-file data from PhEDEx

#        result = self._phedex.make_request('data', ['dataset=' + name, 'level=file'])
        result = self._phedex.make_request('data', ['dataset=' + name, 'level=block'])

        if len(result) == 0:
            return None
        
        dataset_entry = result[0]

        ## Create the dataset object

        dataset = Dataset(dataset_entry['name'])
        if dataset_entry['time_update'] is None:
            dataset.last_update = int(dataset_entry['time_create'])
        else:
            dataset.last_update = int(dataset_entry['time_update'])

        ## Fill block and file data

        for block_entry in dataset_entry['block']:
            name = block_entry['name']
            block_name = Block.translate_name(name[name.find('#') + 1:])
            
            block = Block(
                block_name,
                dataset,
                size = block_entry['bytes'],
                num_files = block_entry['files'],
                is_open = (block_entry['is_open'] == 'y')
            )

            dataset.blocks.add(block)

#            for file_entry in block_entry['file']:
#                lfile = File(
#                    file_entry['lfn'],
#                    block = block,
#                    size = file_entry['bytes']
#                )
#
#                block.files.add(lfile)

        ## Get other details of the dataset from DBS
        # 1. status and PD type

        result = self._dbs.make_request('datasets', ['dataset=' + name, 'dataset_access_type=*', 'detail=True'])
        
        if len(result) != 0:
            dbs_entry = result[0]
            dataset.status = Dataset.status_val(dbs_entry['dataset_access_type'])
            dataset.data_type = Dataset.data_type_val(dbs_entry['primary_ds_type'])
        else:
            dataset.status = Dataset.STAT_UNKNOWN
            dataset.data_type = Dataset.TYPE_UNKNOWN

        # 2. software version

        result = self._dbs.make_request('releaseversions', ['dataset=' + name])
        if len(result) != 0:
            try:
                version = result[0]['release_version'][0]
            except KeyError:
                pass
            else:
                matches = re.match('CMSSW_([0-9]+)_([0-9]+)_([0-9]+)(|_.*)', version)
                if matches:
                    cycle, major, minor = map(int, [matches.group(i) for i in range(1, 4)])
        
                    if matches.group(4):
                        suffix = matches.group(4)[1:]
                    else:
                        suffix = ''
        
                    dataset.software_version = (cycle, major, minor, suffix)

        return dataset

    def get_block(self, name):
        ## Get the full block-file data from PhEDEx

#        result = self._phedex.make_request('data', ['dataset=' + name, 'level=file'])
        result = self._phedex.make_request('data', ['block=' + name, 'level=block'])

        if len(result) == 0:
            return None
        
        dataset_entry = result[0]
        block_entry = dataset_entry['block'][0]

        name = block_entry['name']
        block_name = Block.translate_name(name[name.find('#') + 1:])

        ## Create the block object

        block = Block(
            block_name,
            dataset = Dataset(dataset_entry['name']),
            size = block_entry['bytes'],
            num_files = block_entry['files'],
            is_open = (block_entry['is_open'] == 'y')
        )
        
        return block
