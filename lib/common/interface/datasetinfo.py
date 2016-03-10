class DatasetInfoSourceInterface(object):
    """
    Interface specs for probe to the dataset information source.
    """

    def __init__(self):
        pass

    def get_dataset(self, name):
        """
        Return a list of datasets that match the wildcard name.
        site_filt can be a wildcard string or a list of sites.
        """

        return None


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
