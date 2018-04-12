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

        @return [(exec_id, write_request, title, path, args, user_name)]
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

    def identify_user(self, dn):
        """
        Translate the DN to user account name.
        @param dn   Certificate Distinguished Name.

        @return  User name (string) or None (if not identified)
        """
        raise NotImplementedError('identify_user')

    def authorize_user(self, user, service):
        """
        @param user     User name.
        @param service  Service (role) name user is acting in.
        
        @return boolean
        """
        raise NotImplementedError('authorize_user')

    def check_connection(self):
        """
        @return  True if connection is OK, False if not
        """
        raise NotImplementedError('check_connection')

    def send_heartbeat(self):
        raise NotImplementedError('send_heartbeat')

    def disconnect(self):
        raise NotImplementedError('disconnect')
