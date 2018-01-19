import logging
import fnmatch
import re

from dynamo.source.datasetinfo import DatasetInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.utils.interface.webservice import RESTService
from dynamo.utils.parallel import Map
from dynamo.dataformat import Dataset, Block, File, IntegrityError

LOG = logging.getLogger(__name__)

class PhEDExDatasetInfoSource(DatasetInfoSource):
    """DatasetInfoSource using PhEDEx and DBS."""

    def __init__(self, config):
        DatasetInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.phedex)
        self._dbs = RESTService(config.dbs)

    def get_dataset_names(self, include = ['*'], exclude = []):
        dataset_names = []

        exclude_exps = []
        for pattern in exclude:
            exclude_exps.append(re.compile(fnmatch.translate(pattern)))

        def add_datasets(result):
            for entry in result:
                name = entry['dataset']
                for ex_exp in exclude_exps:
                    if ex_exp.match(name):
                        break
                else:
                    # not excluded
                    dataset_names.append(name)

        if len(include) == 1 and include[0] == '/*/*/*':
            # all datasets requested - will do this efficiently
            result = self._dbs.make_request('acquisitioneras')
            sds = [entry['acquisition_era_name'] for entry in result]

            # query DBS in parallel
            args = [('datasets', ['acquisition_era_name=' + sd]) for sd in sds]
            results = Map().execute(self._dbs.make_request, args)
            for result in results:
                add_datasets(result)

        for in_pattern in include:
            result = self._dbs.make_request('datasets', ['dataset=' + in_pattern])
            add_datasets(result)

        return dataset_names

    def get_updated_datasets(self, updated_since): #override
        LOG.warning('PhEDExDatasetInfoSource can only return a list of datasets and blocks that are created since the given timestamp.')

        result = self._phedex.make_request('data', ['dataset=' + name, 'level=block', 'create_since=%d' % updated_since])

        if len(result) == 0 or 'dataset' not in result[0]:
            return []

        updated_datasets = []
        
        for dataset_entry in result[0]['dataset']:
            dataset = self._create_dataset(dataset_entry)
            updated_datasets.append(dataset)

        return updated_datasets

    def get_dataset(self, name, with_files = False): #override
        ## Get the full dataset-block-file data from PhEDEx

        if with_files:
            level = 'file'
        else:
            level = 'block'

        result = self._phedex.make_request('data', ['dataset=' + name, 'level=' + level])

        try:        
            dataset_entry = result[0]['dataset'][0]
        except:
            return None

        ## Create the dataset object
        dataset = self._create_dataset(dataset_entry)

        ## Fill block and file data
        if 'block' in dataset_entry:
            for block_entry in dataset_entry['block']:
                block = self._create_block(block_entry, dataset)
                dataset.blocks.add(block)

                # size and num_files are left 0 in _create_dataset (PhEDEx does not tell)
                dataset.size += block.size
                dataset.num_files += block.num_files

                if with_files and 'file' in block_entry:
                    files = set()
                    for file_entry in block_entry['file']:
                        files.add(self._create_file(file_entry, block))
        
                    block.files.update(files)
                    # _create_block sets size and num_files; just need to update the files list

        return dataset

    def get_block(self, name, dataset = None, with_files = False): #override
        ## Get the full block-file data from PhEDEx

        if with_files:
            level = 'file'
        else:
            level = 'block'

        result = self._phedex.make_request('data', ['block=' + name, 'level=' + level])

        try:
            dataset_entry = result[0]['dataset'][0]
            block_entry = dataset_entry['block'][0]
        except:
            return None

        if dataset is None:
            link_dataset = False
            # Just need a named object
            dataset = Dataset(dataset_entry['name'])
        else:
            link_dataset = True
            if dataset.name != dataset_entry['name']:
                raise IntegrityError('Inconsistent dataset %s passed to get_block(%s)', dataset.name, name)

        block = self._create_block(block_entry, dataset)

        if with_files and 'file' in block_entry:
            files = set()
            for file_entry in block_entry['file']:
                files.add(self._create_file(file_entry, block))

            block.files.update(files)
            # _create_block sets size and num_files; just need to update the files list

        if link_dataset:
            existing = dataset.find_block(block.name)
            if existing is None:
                dataset.blocks.add(block)
                dataset.size += block.size
                dataset.num_files += block.num_files
            else:
                dataset.blocks.remove(existing)
                dataset.size += block.size - existing.size
                dataset.num_files += block.num_files - existing.num_files

        return block

    def get_file(self, name, block = None):
        ## Get the file data from PhEDEx

        result = self._phedex.make_request('data', ['file=' + name, 'level=file'])

        try:
            block_entry = result[0]['dataset'][0]['block'][0]
            file_entry = block_entry['file'][0]
        except:
            return None

        bname = block_entry['name']
        block_name = Block.to_internal_name(bname[bname.find('#') + 1:])

        if block is None:
            link_block = False
            # Just need a named object
            dataset = Dataset(dataset_entry['name'])
            block = Block(block_name, dataset)
        else:
            link_block = True
            if block.name != block_name:
                raise IntegrityError('Inconsistent block %s passed to get_file(%s)', block.real_name(), name)

        lfile = self._create_file(file_entry, block)

        if link_block:
            # Caution - by adding this file we edit the block properties too

            existing = block.find_file(lfile.fid())
            if existing is None:
                block.add_file(lfile)
            else:
                block.remove_file(existing)
                block.add_file(lfile)

        return lfile

    def get_files(self, block): #override
        files = set()

        result = self._phedex.make_request('data', ['block=' + name.real_name(), 'level=file'])

        try:
            file_entries = result[0]['dataset'][0]['block'][0]['file']
        except:
            return files

        for file_entry in file_entries:
            files.add(self._create_file(file_entry, block))

        return files

    def _create_dataset(self, dataset_entry):
        """
        Create a dataset object with blocks and files from a PhEDEx dataset entry
        """

        dataset = Dataset(
            dataset_entry['name'],
            is_open = (dataset_entry['is_open'] == 'y')
        )

        if 'time_update' in dataset_entry and dataset_entry['time_update'] is not None:
            dataset.last_update = int(dataset_entry['time_update'])
        else:
            dataset.last_update = int(dataset_entry['time_create'])

        ## Get other details of the dataset from DBS
        self._fill_dataset_details(dataset)

        return dataset

    def _create_block(self, block_entry, dataset):
        """
        Create a block object with files from a PhEDEx block entry
        """

        bname = block_entry['name']
        block_name = Block.to_internal_name(bname[bname.find('#') + 1:])
        
        block = Block(
            block_name,
            dataset,
            size = block_entry['bytes'],
            num_files = block_entry['files'],
            is_open = (block_entry['is_open'] == 'y')
        )

        if 'time_update' in block_entry and block_entry['time_update'] is not None:
            block.last_update = int(block_entry['time_update'])
        else:
            block.last_update = int(block_entry['time_create'])

        return block

    def _create_file(self, file_entry, block):
        lfile = File(
            file_entry['lfn'],
            block = block,
            size = file_entry['bytes']
        )

        return lfile

    def _fill_dataset_details(self, dataset):
        # 1. status and PD type

        result = self._dbs.make_request('datasets', ['dataset=' + dataset.name, 'dataset_access_type=*', 'detail=True'])
        
        if len(result) != 0:
            dbs_entry = result[0]
            dataset.status = Dataset.status_val(dbs_entry['dataset_access_type'])
            dataset.data_type = Dataset.data_type_val(dbs_entry['primary_ds_type'])
        else:
            dataset.status = Dataset.STAT_UNKNOWN
            dataset.data_type = Dataset.TYPE_UNKNOWN

        # 2. software version

        result = self._dbs.make_request('releaseversions', ['dataset=' + dataset.name])
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
