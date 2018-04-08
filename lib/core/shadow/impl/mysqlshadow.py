from dynamo.core.shadow.base import MasterShadow
from dynamo.utils.interface import MySQL
from dynamo.dataformat import Configuration

class MySQLMasterShadow(MasterShadow):
    def __init__(self, config):
        MasterShadow.__init__(self, config)

        self._mysql = MySQL(config.db_params)

    def copy(self, master_server):
        all_servers = master_server.get_host_list(detail = True)
        fields = ('hostname', 'last_heartbeat', 'status', 'store_host', 'store_module', 'board_module', 'board_config')
        self._mysql.insert_many('servers', fields, None, all_servers, do_update = True)

        all_users = master_server.get_user_list()
        fields = ('name', 'email', 'dn')
        self._mysql.insert_many('users', fields, None, all_users, do_update = True)

    def get_next_master(self, current):
        self._mysql.query('DELETE FROM `servers` WHERE `hostname` = %s', current)
        
        # shadow config must be the same as master
        result = self._mysql.query('SELECT `hostname`, `shadow_module`, `shadow_config` FROM `servers` ORDER BY `id` LIMIT 1')
        if len(result) == 0:
            raise RuntimeError('No servers can become master at this moment')

        return result[0]
