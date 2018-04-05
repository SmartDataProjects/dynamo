from dynamo.core.board.base import UpdateBoard
from dynamo.core.inventory import DynamoInventory
from dynamo.utils.interface import MySQL

class MySQLUpdateBoard(UpdateBoard):
    def __init__(self, config):
        UpdateBoard.__init__(self, config)

        self._mysql = MySQL(config.db_params)

    def lock(self): #override
        self._mysql.query('LOCK TABLES `inventory_updates` WRITE')

    def unlock(self): #override
        self._mysql.query('UNLOCK TABLES')

    def get_updates(self): #override
        return self._mysql.xquery('SELECT `cmd`, `obj` FROM `inventory_updates` ORDER BY `id`')

    def flush(self): #override
        self._mysql.query('DELETE FROM `inventory_updates`')
        self._mysql.query('ALTER TABLE `inventory_updates` AUTO_INCREMENT = 1')

    def write_updates(self, update_commands): #override
        self._mysql.query('LOCK TABLES `inventory_updates` WRITE')

        try:
            sql = 'INSERT INTO `inventory_updates` (`cmd`, `obj`) VALUES (%s, %s)'

            for cmd, sobj in update_commands:
                if cmd == DynamoInventory.CMD_UPDATE:
                    self._mysql.query(sql, 'update', sobj)
                elif cmd == DynamoInventory.CMD_DELETE:
                    self._mysql.query(sql, 'delete', sobj)

        finally:
            self._mysql.query('UNLOCK TABLES')
