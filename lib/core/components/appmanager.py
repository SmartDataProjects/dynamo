from dynamo.utils.classutil import get_instance
from dynamo.registry.registry import RegistryDatabase

class AppManager(object):
    """
    Object responsible for scheduling applications.
    Authorizer and AppManager are used from multiple threads. The methods should
    therefore be implemented as stateless as possible.
    MasterServer inherits from Authorizer and AppManager.
    """

    _statuses = ['new', 'assigned', 'run', 'done', 'notfound', 'authfailed', 'failed', 'killed']
    STAT_NEW, STAT_ASSIGNED, STAT_RUN, STAT_DONE, STAT_NOTFOUND, STAT_AUTHFAILED, STAT_FAILED, STAT_KILLED = range(1, 9)

    _auth_levels = ['read', 'auth', 'write']
    LV_NOAUTH, LV_AUTH, LV_WRITE = range(1, 4)

    @staticmethod
    def status_name(arg):
        try:
            return AppManager._statuses[arg - 1]
        except:
            return arg

    @staticmethod
    def status_val(arg):
        try:
            return eval('AppManager.STAT_' + arg.upper())
        except:
            return arg

    @staticmethod
    def auth_level_name(arg):
        try:
            return AppManager._auth_levels[arg - 1]
        except:
            return arg

    @staticmethod
    def auth_level_val(arg):
        try:
            return eval('AppManager.LV_' + arg.upper())
        except:
            return arg

    @staticmethod
    def get_instance(module, config):
        return get_instance(AppManager, module, config)

    def __init__(self, config):
        self.readonly_config = None
        if 'applock' in config:
            self.applock = RegistryDatabase(config.applock)
        else:
            self.applock = None

    def get_writing_process_id(self):
        """
        Return the appid of the writing process, or 0 if there is a writing web interface.
        """
        raise NotImplementedError('get_writing_process_id')

    def get_writing_process_host(self):
        """
        Return the host of the writing process or None.
        """
        raise NotImplementedError('get_writing_process_host')

    def get_web_write_process_id(self):
        """
        Return the PID of the web write process.
        """
        raise NotImplementedError('get_web_write_process_id')

    def get_running_processes(self):
        """
        @return  [(title, write_request, host, queued_time)]
        """
        raise NotImplementedError('get_running_processes')

    def schedule_application(self, title, path, args, user_id, host, auth_level):
        """
        Schedule an application to the master server.
        @param title          Application title.
        @param path           Application path.
        @param args           Arguments to the application
        @param user_id        User id of the requester
        @param host           Host name of the requester
        @param auth_level     Authorization level (LV_*)

        @return application id
        """
        raise NotImplementedError('schedule_application')

    def get_next_application(self, read_only):
        """
        @return {appid, write_request, user_name, user_host, title, path, args} or None
        """
        if self.applock:
            blocked_apps = self.applock.get_locked_apps()
        else:
            blocked_apps = []

        return self._do_get_next_application(read_only, blocked_apps)

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

    def start_write_web(self, host, pid):
        """
        Web interfaces are not quite applications, but they require write locks too.
        @param host   Host name of the web server
        @param pid    PID of the writing process
        """
        raise NotImplementedError('start_write_web')

    def stop_write_web(self):
        raise NotImplementedError('stop_write_web')

    def check_application_auth(self, title, user, checksum):
        """
        @param title      Title of the application
        @param user       User of the application
        @param checksum   Checksum of the application file
        
        @return True if the application is authorized to write to the inventory.
        """
        raise NotImplementedError('check_application_auth')

    def list_authorized_applications(self, titles = None, users = None, checksums = None):
        """
        Return the list of write-authorized applications.
        @param title      If given as a list of strings, limit to applications with given titles.
        @param users      If given as a list of strings, limit to applications authorized under given users.
        @param checksums  If given as a list of strings, limit to applications with given checksums.
        """
        raise NotImplementedError('list_authorized_applications')

    def authorize_application(self, title, checksum, user = None):
        """
        Authorize an application to write to inventory. If user = None, authorize for everyone.
        @return True if success, False if not.
        """
        raise NotImplementedError('authorize_application')

    def revoke_application_authorization(self, title, user = None):
        """
        Revoke an app auth.
        @return True if success, False if not.
        """
        raise NotImplementedError('revoke_application_authorization')

    def register_sequence(self, name, user, restart = False):
        """
        Register a scheduled sequence.
        @param name    Name of the sequence
        @param user    Name of the user
        @param restart If True, sequence always starts from line 0

        @return True if success, False if not.
        """
        raise NotImplementedError('register_sequence')

    def find_sequence(self, name):
        """
        Find a sequence with the given name.
        @param name  Name of the sequence

        @return (name, user, restart, enabled) or None
        """
        raise NotImplementedError('find_sequence')

    def update_sequence(self, name, restart = None, enabled = None):
        """
        Toggle the sequence state.
        @param name    Name of the sequence
        @param restart True: sequence starts from line 0
        @param enabled True: sequence enabled, False: disabled

        @return True if success, False if not.
        """
        raise NotImplementedError('update_sequence')

    def delete_sequence(self, name):
        """
        Delete a registered sequence.
        @param name    Name of the sequence

        @return True if success, False if not.
        """
        raise NotImplementedError('delete_sequence')

    def get_sequences(self, enabled_only = True):
        """
        @return [name]
        """
        raise NotImplementedError('get_sequences')

    def create_appmanager(self):
        """
        Clone self with fresh connections. Use readonly_config if available.
        @return A new AppManager instance with a fresh connection
        """
        raise NotImplementedError('create_appmanager')
