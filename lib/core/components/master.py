import logging

LOG = logging.getLogger(__name__)

class Authorizer(object):
    """
    Interface to provide read-only user authorization routines of the master server
    without exposing the server itself.
    Authorizer and MasterServer are used from multiple threads. The methods should
    therefore be implemented as stateless as possible.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, Authorizer):
            raise RuntimeError('%s is not a subclass of Authorizer' % module)

        return cls(config)

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

    def identify_user(self, dn = '', name = '', with_id = False):
        """
        Translate the DN to user account name.
        @param dn     Certificate Distinguished Name.
        @param name   User name.
        @param get_id If true, return a tuple (user name, user id)

        @return  User name string or (user name, user id). None if not identified
        """
        raise NotImplementedError('identify_user')

    def identify_role(self, name, with_id = False):
        """
        Check if a role exists.
        @param name  Role name
        
        @return  Role name string or (role name, role id). None if not identified
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


class AppManager(object):
    """
    Object responsible for scheduling applications.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, AppManager):
            raise RuntimeError('%s is not a subclass of AppManager' % module)

        return cls(config)

    def __init__(self, config):
        pass

    def get_writing_process_id(self):
        raise NotImplementedError('get_writing_process_id')

    def schedule_application(self, title, path, args, user, host, write_request):
        """
        Schedule an application to the master server.
        @param title          Application title.
        @param path           Application path.
        @param args           Arguments to the application
        @param user           User name of the requester
        @param host           Host name of the requester
        @param write_request  Boolean

        @return application id
        """
        raise NotImplementedError('schedule_application')

    def get_next_application(self, read_only):
        """
        @param read_only    Limit to read_only applications
        
        @return {appid, write_request, user_name, user_host, title, path, args} or None
        """
        raise NotImplementedError('get_next_application')

    def get_applications(self, older_than = 0, status = None, app_id = None, path = None):
        """
        Get the list of application entries.
        @param older_than   Return only applications with UNIX time stamps older than the value
        @param status       Return only applications in the given status
        @param app_id       Return application with matching id.
        @param path         Return application at the given path.

        @return [{appid, write_request, user_name, user_host, title, path, args, status, server, exit_code}]
        """
        raise NotImplementedError('get_applications')

    def update_application(self, app_id, **kwd):
        """
        Set the application status.
        
        @param app_id    Application id
        @param kwd       Keyword argument can be status, hostname, exit_code, or path.
        """
        raise NotImplementedError('update_application')

    def delete_application(self, app_id):
        """
        Delete the application record.
        
        @param app_id    Application id
        """
        raise NotImplementedError('delete_application')

    def check_application_auth(self, title, user, checksum):
        raise NotImplementedError('check_application_auth')

    def list_authorized_applications(self, titles = None, users = None, checksums = None):
        """
        Return the list of authorized applications.
        @param title      If given as a list of strings, limit to applications with given titles.
        @param users      If given as a list of strings, limit to applications authorized under given users.
        @param checksums  If given as a list of strings, limit to applications with given checksums.
        """
        raise NotImplementedError('list_authorized_applications')

    def authorize_application(self, title, checksum, user = None):
        """
        Authorize an application. If user = None, authorize for everyone.
        @return True if success, False if not.
        """
        raise NotImplementedError('authorize_application')

    def revoke_application_authorization(self, title, user = None):
        """
        Revoke an app auth.
        @return True if success, False if not.
        """
        raise NotImplementedError('revoke_application_authorization')


class MasterServer(Authorizer, AppManager):
    """
    An interface to the master server that coordinates server activities.
    """

    @staticmethod
    def get_instance(module, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, MasterServer):
            raise RuntimeError('%s is not a subclass of MasterServer' % module)

        return cls(config)

    def __init__(self, config):
        Authorizer.__init__(self, config)
        AppManager.__init__(self, config)

        self.connected = False

    def connect(self):
        """
        Connect to the master server.
        """
        LOG.info('Connecting to master server')

        self._connect()
        self.connected = True

        LOG.info('Master host: %s', self.get_master_host())

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')

    def get_master_host(self):
        """
        @return  Current master server host name.
        """
        raise NotImplementedError('get_master_host')

    def set_status(self, status, hostname):
        raise NotImplementedError('set_status')

    def get_status(self, hostname):
        raise NotImplementedError('get_status')

    def get_host_list(self, status = None, detail = False):
        """
        Get data for all servers connected to this master server.
        @param status   Limit to servers in the given status
        @param detail   boolean
        
        @return If detail = True, list of full info. If detail = False, [(hostname, status, has_store)]
        """
        raise NotImplementedError('get_host_list')

    def copy(self, remote_master):
        """
        When acting as a local shadow of a remote master server, copy the remote content to local.
        @param remote_master  MasterServer instance of the remote server.
        """
        raise NotImplementedError('copy')

    def get_next_master(self, current):
        """
        Return the shadow module name and configuration of the server next-in-line from the current master.
        @return  (shadow module, shadow config)
        """
        raise NotImplementedError('get_next_master')

    def advertise_store(self, module, config):
        raise NotImplementedError('advertise_store')

    def advertise_store_version(self, version):
        raise NotImplementedError('advertise_store_version')

    def get_store_config(self, hostname):
        """
        @param hostname  Remote host name.
        @return (module, config, version)
        """
        raise NotImplementedError('get_store_config')

    def advertise_board(self, module, config):
        raise NotImplementedError('advertise_board')

    def get_board_config(self, hostname):
        raise NotImplementedError('get_board_config')

    def declare_remote_store(self, hostname):
        raise NotImplementedError('declare_remote_store')

    def add_user(self, name, dn, email = None):
        """
        Add a new user.
        @param name  User name
        @param dn    User DN
        @param email User email

        @return True if success, False if not.
        """
        raise NotImplementedError('add_user')

    def update_user(self, name, dn = None, email = None):
        """
        Update data on user.
        @param name   User name
        @param dn     New DN
        @param email  New email
        """
        raise NotImplementedError('update_user')

    def delete_user(self, name):
        """
        Delete a user.
        @param name   User name
        """
        raise NotImplementedError('delete_user')

    def add_role(self, name):
        """
        Add a new role.
        @param name  Role name

        @return True if success, False if not.
        """
        raise NotImplementedError('add_role')

    def authorize_user(self, user, role, target):
        """
        Add (user, role) to authorization list.
        @param user    User name.
        @param role    Role (role) name user is acting in. If None, authorize the user under all roles.
        @param target  Authorization target. If None, authorize the user for all targets.

        @return True if success, False if not.
        """
        raise NotImplementedError('authorize_user')

    def revoke_user_authorization(self, user, role, target):
        """
        Revoke authorization on target from (user, role).
        @param user    User name.
        @param role    Role (role) name user is acting in. If None, authorize the user under all roles.
        @param target  Authorization target. If None, authorize the user for all targets.

        @return True if success, False if not.
        """
        raise NotImplementedError('revoke_user_authorization')

    def create_authorizer(self):
        """
        @return A new authorizer instance with a fresh connection
        """
        raise NotImplementedError('create_authorizer')

    def check_connection(self):
        """
        @return  True if connection is OK, False if not
        """
        raise NotImplementedError('check_connection')

    def send_heartbeat(self):
        raise NotImplementedError('send_heartbeat')

    def disconnect(self):
        raise NotImplementedError('disconnect')
