from dynamo.utils.classutil import get_instance

class Authorizer(object):
    """
    Interface to provide read-only user authorization routines of the master server
    without exposing the server itself.
    Authorizer and AppManager are used from multiple threads. The methods should
    therefore be implemented as stateless as possible.
    MasterServer inherits from Authorizer and AppManager.
    """

    @staticmethod
    def get_instance(module, config):
        return get_instance(Authorizer, module, config)

    def __init__(self, config):
        pass

    def user_exists(self, name):
        """
        Check if a user exists.
        @param name  User name
        
        @return boolean
        """
        raise NotImplementedError('user_exists')

    def list_users(self):
        """
        @return  [(name, dn, email)]
        """
        raise NotImplementedError('list_users')

    def identify_user(self, dn = '', check_trunc = False, name = '', uid = None):
        """
        Translate the DN to user account name.
        @param dn           Certificate Distinguished Name.
        @param check_trunc  Retry progressively truncated DNs until a match is found.
        @param name         User name.
        @param uid          User id.

        @return  (user name, user id, user dn) or None if not identified
        """
        raise NotImplementedError('identify_user')

    def identify_role(self, name):
        """
        Check if a role exists.
        @param name  Role name
        
        @return  (role name, role id) or None if not identified
        """
        raise NotImplementedError('identify_role')

    def list_roles(self):
        """
        @return  List of role names
        """
        raise NotImplementedError('list_roles')

    def list_authorization_targets(self):
        """
        @return List of authorization targets.
        """
        raise NotImplementedError('list_authorization_targets')

    def check_user_auth(self, user, role, target):
        """
        Check the authorization on target for (user, role)
        @param user    User name.
        @param role    Role (role) name user is acting in. If None, authorize the user under all roles.
        @param target  Authorization target. If None, authorize the user for all targets.

        @return boolean
        """
        raise NotImplementedError('check_user_auth')

    def list_user_auth(self, user):
        """
        @param user    User name.
        
        @return [(role, target)]
        """
        raise NotImplementedError('list_user_auth')

    def list_authorized_users(self, target):
        """
        @param target Authorization target. Pass None to get the list of users authorized for all targets.

        @return List of (user name, role name) authorized for the target.
        """
        raise NotImplementedError('list_authorized_users')

    def create_authorizer(self):
        """
        Clone self with fresh connections.
        @return A new authorizer instance with a fresh connection
        """
        raise NotImplementedError('create_authorizer')
