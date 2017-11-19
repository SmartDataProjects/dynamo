class GroupInfoSource(object):
    """
    Interface specs for group information authority.
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

        if hasattr(config, 'dataset_level_groups'):
            # List of group names where olevel should be Dataset
            self.dataset_level_groups = list(config.dataset_level_groups)
        else:
            self.dataset_level_groups = []

    def get_group(self, name):
        """
        @param name  Name of the group
        @return  A Group object with full info, or None if the group is not found.
        """
        raise NotImplementedError('get_group')

    def get_group_list(self):
        """
        @return  List of unlinked Group objects
        """
        raise NotImplementedError('get_group_list')
