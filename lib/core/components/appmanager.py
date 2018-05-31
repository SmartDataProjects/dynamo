from dynamo.utils.classutil import get_instance

class AppManager(object):
    """
    Object responsible for scheduling applications.
    Authorizer and AppManager are used from multiple threads. The methods should
    therefore be implemented as stateless as possible.
    MasterServer inherits from Authorizer and AppManager.
    """

    _statuses = ['new', 'assigned', 'run', 'done', 'notfound', 'authfailed', 'failed', 'killed']
    STAT_NEW, STAT_ASSIGNED, STAT_RUN, STAT_DONE, STAT_NOTFOUND, STAT_AUTHFAILED, STAT_FAILED, STAT_KILLED = range(1, 9)

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
    def get_instance(module, config):
        return get_instance(AppManager, module, config)

    def __init__(self, config):
        pass

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

    def start_write_web(self, host):
        """
        Web interfaces are not quite applications, but they require write locks too.
        @param host   Host name of the web server
        """
        raise NotImplementedError('start_write_web')

    def stop_write_web(self):
        raise NotImplementedError('stop_write_web')

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
