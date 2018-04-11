class UserInfoSource(object):
    """
    Interface specs for user data authority.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.source.impl as impl
        cls = getattr(impl, module)

        if not issubclass(cls, UserInfoSource):
            raise RuntimeError('%s is not a subclass of UserInfoSource' % module)

        return cls(config)

        
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
