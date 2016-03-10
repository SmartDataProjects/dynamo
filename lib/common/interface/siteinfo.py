class SiteInfoSourceInterface(object):
    """
    Interface specs for probe to the site information source.
    """

    def __init__(self):
        pass

    def get_site_list(self, filt = '*'):
        """
        Return a list of sites that match the wildcard name.
        Arguments:
          filt: a wildcard string or a list of wildcard strings.
        """

        return []

    def get_group_list(self, filt = '*'):
        """
        Return a list of groups that match the wildcard name.
        Arguments:
          filt: a wildcard string or a list of wildcard strings.
        """

        return []


if __name__ == '__main__':

    from argparse import ArgumentParser
    import common.interface.classes as classes

    parser = ArgumentParser(description = 'Site information source interface')

    parser.add_argument('command', metavar = 'COMMAND', nargs = '+', help = 'Command to execute.')
    parser.add_argument('--class', '-c', metavar = 'CLASS', dest = 'class_name', default = '', help = 'SiteInfoSourceInterface class to be used.')

    args = parser.parse_args()

    command = args.command[0]
    cmd_args = args.command[1:]

    if args.class_name == '':
        interface = classes.default_interface['site_source']()
    else:
        interface = getattr(classes, args.class_name)()

    if command == 'list':
        if len(cmd_args) != 0:
            print interface.get_site_list(cmd_args[0]).keys()
        else:
            print interface.get_site_list().keys()
