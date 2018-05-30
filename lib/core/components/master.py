import multiprocessing
import logging

from dynamo.core.components.authorizer import Authorizer
from dynamo.core.components.appmanager import AppManager
from dynamo.core.components.host import ServerHost
from dynamo.utils.classutil import get_instance

LOG = logging.getLogger(__name__)

class MasterServer(Authorizer, AppManager):
    """
    An interface to the master server that coordinates server activities. The single instance of the MasterServer
    owned by the ServerManager (owned by DynamoServer) is used by DynamoServer, WebServer, and the subprocesses of the
    WebServer. To avoid interference, all methods of the MasterServer is decorated with a locking mechanism. See
    below the class definition.
    """

    @staticmethod
    def get_instance(module, config):
        return get_instance(MasterServer, module, config)

    def __init__(self, config):
        Authorizer.__init__(self, config)
        AppManager.__init__(self, config)

        self.connected = False

        self._master_server_lock = multiprocessing.RLock()

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
        @param status   Limit to servers in the given status (ServerHost.STAT_*)
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

    def inhibit_write(self):
        return len(self.get_host_list(status = ServerHost.STAT_STARTING)) != 0 or \
            self.get_writing_process_id() is not None

import types

# Decorate all public methods of MasterServer with the lock
def call_with_lock(mthd):
    def wrapper(*args, **kwd):
        # args[0] is self
        with args[0]._master_server_lock:
            return mthd(*args, **kwd)

    return wrapper

for name in dir(MasterServer):
    if not name.startswith('_'):
        mthd = getattr(MasterServer, name)
        if callable(mthd) and not isinstance(mthd, types.FunctionType): # static methods are instances of FunctionType
            setattr(MasterServer, name, call_with_lock(mthd))
