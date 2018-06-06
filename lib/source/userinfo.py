from dynamo.utils.classutil import get_instance

class UserInfoSource(object):
    """
    Interface specs for user data authority.
    """

    @staticmethod
    def get_instance(module, config):
        return get_instance(UserInfoSource, module, config)
        
    def __init__(self, config):
        pass

    def get_user(self, name):
        """
        @param name  Name of the user
        @return A tuple (name, email, DN) of the user. If user is not found, return None.
        """
        raise NotImplementedError('get_user')

    def get_user_list(self):
        """
        @return  {name: (name, email, DN)}
        """
        raise NotImplementedError('get_user_list')
