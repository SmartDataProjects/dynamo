class DatasetInfoSource(object):
    """
    Interface specs for probe to the dataset information source.
    """

    def __init__(self, config):
        if hasattr(config, 'include'):
            if type(config.include) is list:
                self.include = list(config.include)
            else:
                self.include = [config.include]
        else:
            self.include = None

        if hasattr(config, 'exclude'):
            if type(config.exclude) is list:
                self.exclude = list(config.exclude)
            else:
                self.exclude = [config.exclude]
        else:
            self.exclude = None

    def get_dataset(self, name):
        """
        Get a linked set of Dataset-Blocks-Files with full information.
        @param name  Name of dataset
        @return  Dataset with full list of Blocks and Files
        """
        raise NotImplementedError('get_dataset')

    def get_block(self, name):
        """
        Get a linked set of Blocks-Files with full information.
        @param name  Name of block
        @return  Block with full list of Files
        """
        raise NotImplementedError('get_block')

    def get_file(self, name):
        """
        Get a File object.
        @param name  Name of file
        @return  File
        """
        raise NotImplementedError('get_file')
