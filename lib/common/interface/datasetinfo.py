class DatasetInfoSourceInterface(object):
    """
    Interface specs for probe to the dataset information source.
    """

    def __init__(self):
        pass

    def get_dataset(self, name):
        """
        Construct a dataset of the given name.
        """

        return None

    def get_datasets(self, names):
        """
        Return a list of datasets from a list of names. Derived classes can implement a more
        efficient algorithm.
        """

        result = []
        for name in names:
            result.append(self.get_dataset(name))

        return result


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
