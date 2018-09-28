import multiprocessing
import logging
import socket
import types

from dynamo.core.components.authorizer import Authorizer
from dynamo.core.components.appmanager import AppManager
from dynamo.core.components.host import ServerHost, OutOfSyncError
from dynamo.utils.classutil import get_instance

LOG = logging.getLogger(__name__)

class MasterServer(Authorizer, AppManager):
    """
    An interface to the master server that coordinates server activities. The single instance of the MasterServer
    owned by the ServerManager (owned by DynamoServer) is used by DynamoServer, WebServer, and the subprocesses of the
    WebServer. To avoid interference, all public methods of the instance of the MasterServer is decorated with a wrapper
    that locks the execution. A secondary Dynamo server has two instances of MasterServer: one connected to the primary
    server, and the other as a local shadow.
    """

    @staticmethod
    def get_instance(module, config):
        instance = get_instance(MasterServer, module, config)

        # Decorate all public methods of MasterServer with the lock. Without this, the lock() method is meaningless.
        # Subclasses may have its own lock placed on the actual shared resource (e.g. database) by _do_lock, in which
        # case this class-level lock is redundant.
        
        for name in dir(instance):
            if name == 'lock' or name == 'unlock' or name.startswith('_'):
                continue

            mthd = getattr(instance, name)
            if callable(mthd) and not isinstance(mthd, types.FunctionType): # static methods are instances of FunctionType
                setattr(instance, name, instance._make_wrapper(mthd))

        return instance

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

        self.lock()

        try:
            self._connect()
    
            if self.get_host() == 'localhost' or self.get_host() == socket.gethostname():
                # This is the master host; make sure there are no dangling web writes
                writing = self.get_writing_process_id()
                if writing == 0:
                    self.stop_write_web()
                elif writing > 0:
                    self.update_application(writing, status = AppManager.STAT_KILLED)
                
        finally:
            self.unlock()

        self.connected = True

        LOG.info('Master host: %s', self.get_host())

    def lock(self):
        self._master_server_lock.acquire()
        self._do_lock()

    def unlock(self):
        self._do_unlock()
        self._master_server_lock.release()

    def get_host(self):
        """
        @return  Name of the host this instance is connected to. Must not raise.
        """
        raise NotImplementedError('get_host')

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
        @param target  Authorization target. If None, revoke for all targets.

        @return True if success, False if not.
        """
        raise NotImplementedError('revoke_user_authorization')

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

    def _make_wrapper(self, method):
        """
        Return a wrapper function with operations that should be applied to all public methods of this class.
        In this default implementation, we place a process lock and try executing the method. If an exception
        is thrown, check the connection to the master server (unless this instance is the master), and raise
        an OutOfSyncError if the connection is lost.
        """

        def wrapper(*args, **kwd):
            with self._master_server_lock:
                try:
                    return method(*args, **kwd)
                except:
                    if self.get_host() == 'localhost' or self.get_host() == socket.gethostname():
                        raise
                    elif not self.check_connection():
                        raise OutOfSyncError('Lost connection to remote master')
                    else:
                        raise

        return wrapper
