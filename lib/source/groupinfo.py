class GroupInfoSource(object):
    """
    Interface specs for group information authority.
    """

    def __init__(self, config):
        include = config.get('include', None)

        if type(include) is list:
            self.include = list(include)
        elif include is not None:
            self.include = [include]
        else:
            self.include = None

        exclude = config.get('exclude', None)

        if type(exclude) is list:
            self.exclude = list(exclude)
        elif exclude is not None:
            self.exclude = [exclude]
        else:
            self.exclude = None

        # List of group names where olevel should be Dataset
        self.dataset_level_groups = list(config.get('dataset_level_groups', []))

    def get_group(self, name):
        """
        @param name  Name of the group
        @return  A Group object with full info, or None if the group is not found.
        """
        raise NotImplementedError('get_group')

    def get_group_list(self):
        """
        @return  List of unlinked Group objects. Will always contain a null group.
        """
        raise NotImplementedError('get_group_list')
