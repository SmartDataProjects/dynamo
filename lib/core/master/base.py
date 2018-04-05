class MasterServer(object):
    """
    An interface to the master server that coordinates server activities.
    """

    def __init__(self, config):
        pass

    def lock(self):
        raise NotImplementedError('lock')

    def unlock(self):
        raise NotImplementedError('unlock')

    def set_status(self, status, hostname):
        raise NotImplementedError('set_status')

    def get_status(self, hostname):
        raise NotImplementedError('get_status')

    def get_host_list(self, status = None):
        raise NotImplementedError('get_host_list')

    def get_writing_process_id(self):
        raise NotImplementedError('get_writing_process_id')

    def schedule_application(self, title, path, args, user, write_request):
        raise NotImplementedError('schedule_application')

    def get_next_application(self, read_only):
        raise NotImplementedError('get_next_application')

    def set_application_status(self, status, app_id, hostname = None, exit_code = None):
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

    def send_heartbeat(self):
        raise NotImplementedError('send_heartbeat')

    def disconnect(self):
        raise NotImplementedError('disconnect')
