class UserInfoSource(object):
    """
    Interface specs for user data authority.
    """
        
    def __init__(self, config):
        pass

    def get_user(self, name):
        """
        @param name  Name of the user
        @return A tuple (name, email, DN) of the user. If user is not found, return None.
        """
        raise NotImplementedError('get_user')

    def get_user_list(self, users, filt = '*'):
        """
        REVIEW THIS
        Fill the given list with users with names matching the filter.
        @param users   {name: userdata} to be filled with user information in the expected format.
        @param filt    Wildcard pattern.
        """
        raise NotImplementedError('get_user_list')
