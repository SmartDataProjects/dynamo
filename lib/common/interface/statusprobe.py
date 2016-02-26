class StatusProbeInterface(object):
    """
    Interface specs for probe to the system status. Collects information from
    available sources and parse it in a form that can be stored in the inventory
    database.
    """

    def __init__(self):
        pass

    def get_data(self, site = '', dataset = '/*/*/*'):

        sites = self.get_site_list(site)
        datasets = self.get_dataset_list(dataset)
        self.make_replica_links(sites, datasets)

        return sites, datasets

    def get_site_list(self, name = ''):
        """Return a list of sites that match the wildcard name."""

        return {}

    def get_dataset_list(self, name = '/*/*/*'):
        """Return a list of datasets that match the wildcard name."""

        return {}

    def make_replica_links(self, sites, datasets):
        """Link the sites with datasets and blocks"""
        pass


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Status probe interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'StatusProbeInterface class to be used.')

    args = parser.parse_args()

    command = args.command[0]
    cmd_args = args.command[1:]

    if args.class_name == '':
        interface = classes.default_interface['status_probe']()
    else:
        interface = getattr(classes, args.class_name)()

    if command == 'sites':
        print interface.get_site_list(cmd_args[0]).keys()

    elif command == 'datasets':
        print interface.get_dataset_list(cmd_args[0]).keys()
