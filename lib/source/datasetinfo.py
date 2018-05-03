import fnmatch
import re
import logging

LOG = logging.getLogger(__name__)

class DatasetInfoSource(object):
    """
    Interface specs for probe to the dataset information source.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.source.impl as impl
        cls = getattr(impl, module)

        if not issubclass(cls, DatasetInfoSource):
            raise RuntimeError('%s is not a subclass of DatasetInfoSource' % module)

        return cls(config)


    def __init__(self, config):
        if hasattr(config, 'include'):
            if type(config.include) is list:
                self.include = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.include)
            else:
                self.include = [re.compile(fnmatch.translate(config.include))]
        else:
            self.include = None

        if hasattr(config, 'exclude'):
            if type(config.exclude) is list:
                self.exclude = map(lambda pattern: re.compile(fnmatch.translate(pattern)), config.exclude)
            else:
                self.exclude = [re.compile(fnmatch.translate(config.exclude))]
        else:
            self.exclude = None

    def get_dataset_names(self, include = ['*'], exclude = []):
        """
        Return a list of dataset names from the include and exclude patterns.
        
        @param include  List of fnmatch patterns of the dataset names to be included.
        @param exclude  List of fnmatch patterns to exclude from the included list.
        """
        raise NotImplementedError('get_dataset_names')

    def get_updated_datasets(self, updated_since):
        """
        Get a list of updated Datasets-Blocks-Files with full information.
        @param updated_since  Unix timestamp
        @return  List of datasets
        """
        raise NotImplementedError('get_updated_datasets')

    def get_dataset(self, name, with_files = False):
        """
        Get a linked structure of Dataset-Blocks-Files with full information.
        @param name  Name of dataset
        @return  Dataset with full list of Blocks and Files
        """
        raise NotImplementedError('get_dataset')

    def get_block(self, name, dataset = None, with_files = False):
        """
        Get a linked set of Blocks-Files with full information.
        @param name     Name of block
        @param dataset  If not None, link the block against this dataset.
        @return  Block with full list of Files
        """
        raise NotImplementedError('get_block')

    def get_file(self, name, block = None):
        """
        Get a File object.
        @param name  Name of file
        @param block If not None, link the file against this block.
        @return  File
        """
        raise NotImplementedError('get_file')

    def get_files(self, dataset_or_block):
        """
        Get a set of File objects. Files will not be linked from the block.
        @param dataset_or_block  Dataset or Block object
        @return set of Files
        """
        raise NotImplementedError('get_files')

    def check_allowed_dataset(self, dataset_name):
        if self.include is not None:
            for pattern in self.include:
                if pattern.match(dataset_name):
                    break
            else:
                # no match
                LOG.debug('Dataset %s is not in include list.', dataset_name)
                return False

        if self.exclude is not None:
            for pattern in self.exclude:
                if pattern.match(dataset_name):
                    LOG.debug('Dataset %s is in exclude list.', dataset_name)
                    return False

        return True
