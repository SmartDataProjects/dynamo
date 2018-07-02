from dynamo.utils.classutil import get_instance
from dynamo.dataformat import Configuration

class UserInfoSource(object):
    """
    Interface specs for user data authority.
    """

    @staticmethod
    def get_instance(module = None, config = None):
        if module is None:
            module = UserInfoSource._module
        if config is None:
            config = UserInfoSource._config

        return get_instance(UserInfoSource, module, config)

    _module = ''
    _config = Configuration()

    @staticmethod
    def set_default(config):
        UserInfoSource._module = config.module
        UserInfoSource._config = config.config
        
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
