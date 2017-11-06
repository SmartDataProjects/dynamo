class UserInfoSourceInterface(object):
    """
    Interface specs for user data authority.
    """
        
    def __init__(self):
        pass

    def get_user(self, name):
        """
        Return info of a single user in the expected format. Return None for invalid user name.
        """
        pass

    def get_user_list(self, users, filt = '*'):
        """
        Fill the given list with users with names matching the filter.
        @param users   {name: userdata} to be filled with user information in the expected format.
        @param filt    Wildcard pattern.
        """
        pass
