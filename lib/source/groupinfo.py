class GroupInfoSourceInterface(object):
    """
    Interface specs for group information authority.
    """

    def __init__(self):
        pass

    def get_group_list(self, groups, filt = '*'):
        """
        Fill the list of groups with groups that match the wildcard name.

        @param groups  {name: Group} to be filled. Information of the groups already in the list will be updated.
        @param filt    A wildcard string or a list of wildcard strings.
        """
        pass
