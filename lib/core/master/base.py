class MasterServer(object):
    """
    An interface to the master server that coordinates server activities.
    """

    def __init__(self, config):
        self.master_host = config.host
        self.connected = False

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')

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

    def set_application_status(self, status, app_id, hostname = None, exit_code = None):
        """
        Set the application status.

        @param status    New status        
        @param app_id    Application id
        @param exit_code Exit code (optional)
        """
        raise NotImplementedError('set_application_status')

    def get_application_status(self, app_id):
        raise NotImplementedError('get_application_status')

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
