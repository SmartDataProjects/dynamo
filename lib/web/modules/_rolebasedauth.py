from dynamo.web.exceptions import AuthorizationError
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin

class RoleBasedAuthMixin(MySQLRegistryMixin):
    def __init__(self, config, table_name):
        MySQLRegistryMixin.__init__(self, config)
        # Name of the table to look up authorization
        self.table_name = table_name

    def authorize(caller, role):
        sql = 'SELECT COUNT(*) FROM `{table}` WHERE `user_id` = %s AND `role_id` = (SELECT `name` FROM `roles` WHERE `name` = %s)'.format(table = self.table_name)
        if self.registry.query(sql, caller.id, role)[0] == 0:
            raise AuthorizationError()
