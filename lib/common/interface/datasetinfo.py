class DatasetInfoSourceInterface(object):
    """
    Interface specs for probe to the dataset information source.
    """

    def __init__(self):
        pass

    def get_dataset(self, name, datasets):
        """
        Construct a dataset of the given name.
        Arguments
         name: the name of the dataset
         datasets: name->dataset dict of known datasets
        """
        pass

    def get_datasets(self, names, datasets):
        """
        Return a list of datasets from a list of names. Derived classes can implement a more
        efficient algorithm.
        Arguments
         names: the names of the datasets
         datasets: name->dataset dict of known datasets
        """

        for name in names:
            self.get_dataset(name, datasets)

    def set_dataset_constituent_info(self, datasets):
        """
        Find information on blocks that constitute the datasets
        """
        pass

    def set_dataset_details(self, datasets):
        """
        Set detailed information, primarily those that may be updated.
        """
        pass


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Dataset information source interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'DatasetInfoSourceInterface class to be used.')

    args = parser.parse_args()

    ds_name = args.command[0]

    if args.class_name == '':
        interface = classes.default_interface['dataset_source']()
    else:
        interface = getattr(classes, args.class_name)()

    print interface.get_dataset(ds_name)
