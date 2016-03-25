class SiteInfoSourceInterface(object):
    """
    Interface specs for probe to the site information source.
    """

    def __init__(self):
        pass

    def get_site_list(self, sites, filt = '*'):
        """
        Fill the list of sites with sites that match the wildcard name.
        Arguments:
          sites: the name->site dict to be filled. Information of the sites already in the list will be updated.
          filt: a wildcard string or a list of wildcard strings.
        """

    def get_group_list(self, groups, filt = '*'):
        """
        Fill the list of groups with groups that match the wildcard name.
        Arguments:
          groups: the name->group dict to be filled. Information of the groups already in the list will be updated.
          filt: a wildcard string or a list of wildcard strings.
        """


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
        results = {}
        if len(cmd_args) != 0:
            interface.get_site_list(results, cmd_args[0])
        else:
            interface.get_site_list(results)

        print results.keys()
