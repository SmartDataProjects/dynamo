import logging

LOG = logging.getLogger(__name__)

class MasterServer(object):
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

    def get_user_list(self):
        """
        Get data for all users.
        @return [(username, email, dn)]
        """
        raise NotImplementedError('get_user_list')

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

    def get_writing_process_id(self):
        raise NotImplementedError('get_writing_process_id')

    def schedule_application(self, title, path, args, user, write_request):
        """
        Schedule an application to the master server.
        @param title          Application title.
        @param path           Application path.
        @param args           Arguments to the application
        @param user           User name of the requester
        @param write_request  Boolean

        @return application id
        """
        raise NotImplementedError('schedule_application')

    def get_next_application(self, read_only):
        raise NotImplementedError('get_next_application')

    def get_applications(self, older_than = 0, has_path = True, app_id = None):
        """
        Get the list of application entries.
        @param older_than   Return only applications older than N seconds
        @param has_path     Return applications whose path is not NULL
        @param app_id       Return application with matching id.

        @return {appid, write_request, user_name, title, path, args, status, server, exit_code}
        """
        raise NotImplementedError('get_applications')

    def update_application(self, app_id, **kwd):
        """
        Set the application status.
        
        @param app_id    Application id
        @param kwd       Keyword argument can be status, hostname, exit_code, or path.
        """
        raise NotImplementedError('update_application')

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

    def advertise_store(self, module, config):
        raise NotImplementedError('advertise_store')

    def get_store_config(self, hostname):
        raise NotImplementedError('get_store_config')

    def advertise_board(self, module, config):
        raise NotImplementedError('advertise_board')

    def get_board_config(self, hostname):
        raise NotImplementedError('get_board_config')

    def declare_remote_store(self, hostname):
        raise NotImplementedError('declare_remote_store')

    def user_exists(self, name):
        """
        Check if a user exists.
        @param name  User name
        
        @return boolean
        """
        raise NotImplementedError('user_exists')

    def identify_user(self, dn, with_id = False):
        """
        Translate the DN to user account name.
        @param dn     Certificate Distinguished Name.
        @param get_id If true, return a tuple (user name, user id)

        @return  User name string or (user name, user id). None if not identified
        """
        raise NotImplementedError('identify_user')

    def add_user(self, name, dn, email = None):
        """
        Add a new user.
        @param name  User name
        @param dn    User DN
        @param email User email

        @return True if success, False if not.
        """
        raise NotImplementedError('add_user')

    def service_exists(self, name):
        """
        Check if a service exists.
        @param name  Service name
        
        @return boolean
        """
        raise NotImplementedError('service_exists')

    def add_service(self, name):
        """
        Add a new service.
        @param name  Service name

        @return True if success, False if not.
        """
        raise NotImplementedError('add_service')

    def is_authorized_user(self, user, service):
        """
        @param user     User name.
        @param service  Service (role) name user is acting in.
        
        @return boolean
        """
        raise NotImplementedError('is_authorized_user')

    def authorize_user(self, user, service):
        """
        Add (user, service) to authorization list.
        @param user     User name.
        @param service  Service (role) name user is acting in.

        @return True if success, False if not.
        """
        raise NotImplementedError('authorize_user')

    def list_authorized_users(self):
        """
        Get the full authorization list.
        @return [(user, service)]
        """
        raise NotImplementedError('list_authorized_users')

    def check_connection(self):
        """
        @return  True if connection is OK, False if not
        """
        raise NotImplementedError('check_connection')

    def send_heartbeat(self):
        raise NotImplementedError('send_heartbeat')

    def disconnect(self):
        raise NotImplementedError('disconnect')
