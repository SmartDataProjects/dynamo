"""
Generic MySQL interface (for an interface).
"""

import MySQLdb
import MySQLdb.converters
import MySQLdb.cursors
import sys
import logging
import time
import re
import traceback

import common.configuration as config

# Fix for some (newer) versions of MySQLdb
from types import TupleType, ListType
MySQLdb.converters.conversions[TupleType] = MySQLdb.converters.escape_sequence
MySQLdb.converters.conversions[ListType] = MySQLdb.converters.escape_sequence

logger = logging.getLogger(__name__)

class MySQL(object):

    @staticmethod
    def escape_string(string):
        return MySQLdb.escape_string(string)
    
    def __init__(self, host = '', user = '', passwd = '', config_file = '', config_group = '', db = ''):
        self._connection_parameters = {}
        if config_file:
            self._connection_parameters['read_default_file'] = config_file
            self._connection_parameters['read_default_group'] = config_group
        if host:
            self._connection_parameters['host'] = host
        if user:
            self._connection_parameters['user'] = user
        if passwd:
            self._connection_parameters['passwd'] = passwd
        if db:
            self._connection_parameters['db'] = db

        self._connection = MySQLdb.connect(**self._connection_parameters)

    def db_name(self):
        return self._connection_parameters['db']

    def close(self):
        self._connection.close()

    def query(self, sql, *args):
        """
        Execute an SQL query. If the query is an INSERT, return the inserted row id (0 if no insertion happened).
        If the query is a SELECT, return an array of:
         - tuples if multiple columns are called
         - values if one column is called
        """

        cursor = self._connection.cursor()

        if logger.getEffectiveLevel() == logging.DEBUG:
            if len(args) == 0:
                logger.debug(sql)
            else:
                logger.debug(sql + ' % ' + str(args))

        try:
            for attempt in range(10):
                try:
                    cursor.execute(sql, args)
                    break
                except MySQLdb.OperationalError:
                    logger.error(str(sys.exc_info()[1]))
                    last_except = sys.exc_info()[1]
                    # reconnect to server
                    cursor.close()
                    self._connection = MySQLdb.connect(**self._connection_parameters)
                    cursor = self._connection.cursor()
    
            else: # 10 failures
                logger.error('Too many OperationalErrors. Last exception:')
                raise last_except

        except:
            logger.error('There was an error executing the following statement:')
            logger.error(sql[:10000])
            logger.error(sys.exc_info()[1])
            raise

        result = cursor.fetchall()

        if cursor.description is None:
            if cursor.lastrowid != 0:
                # insert query
                return cursor.lastrowid
            else:
                return cursor.rowcount

        elif len(result) != 0 and len(result[0]) == 1:
            # single column requested
            return [row[0] for row in result]

        else:
            return list(result)

    def xquery(self, sql, *args):
        """
        Execute an SQL query. If the query is an INSERT, return the inserted row id (0 if no insertion happened).
        If the query is a SELECT, return an iterator of:
         - tuples if multiple columns are called
         - values if one column is called
        """

        cursor = self._connection.cursor(MySQLdb.cursors.SSCursor)

        if logger.getEffectiveLevel() == logging.DEBUG:
            if len(args) == 0:
                logger.debug(sql)
            else:
                logger.debug(sql + ' % ' + str(args))

        try:
            for attempt in range(10):
                try:
                    cursor.execute(sql, args)
                    break
                except MySQLdb.OperationalError:
                    logger.error(str(sys.exc_info()[1]))
                    last_except = sys.exc_info()[1]
                    # reconnect to server
                    cursor.close()
                    self._connection = MySQLdb.connect(**self._connection_parameters)
                    cursor = self._connection.cursor(MySQLdb.cursors.SSCursor)
    
            else: # 10 failures
                logger.error('Too many OperationalErrors. Last exception:')
                raise last_except

        except:
            logger.error('There was an error executing the following statement:')
            logger.error(sql[:10000])
            logger.error(sys.exc_info()[1])
            raise

        if cursor.description is None:
            if cursor.lastrowid != 0:
                # insert query
                return cursor.lastrowid
            else:
                return cursor.rowcount

        row = cursor.fetchone()
        if row is None:
            cursor.close()
            return # having yield statements below makes this a 0-element iterator

        single_column = (len(row) == 1)

        while row:
            if single_column:
                yield row[0]
            else:
                yield row

            row = cursor.fetchone()

        cursor.close()
        return

    def execute_many(self, sqlbase, key, pool, additional_conditions = [], exec_on_match = True, order_by = ''):
        result = []

        if type(key) is tuple:
            key_str = '(' + ','.join('`%s`' % k for k in key) + ')'
        elif '`' not in key:
            key_str = '`%s`' % key
        else:
            key_str = key

        sqlbase += ' WHERE '
        for add in additional_conditions:
            sqlbase += add + ' AND '
        sqlbase += key_str
        if exec_on_match:
            sqlbase += ' IN '
        else:
            sqlbase += ' NOT IN '

        def execute(pool_expr):
            sql = sqlbase + pool_expr
            if order_by:
                sql += ' ORDER BY ' + order_by

            vals = self.query(sql)
            if type(vals) is list:
                result.extend(vals)

        self._execute_in_batches(execute, pool)

        return result

    def select_many(self, table, fields, key, pool, additional_conditions = [], select_match = True, order_by = ''):
        if type(fields) is str:
            fields_str = '`%s`' % fields
        else:
            fields_str = ','.join('`%s`' % f for f in fields)

        sqlbase = 'SELECT {fields} FROM `{table}`'.format(fields = fields_str, table = table)

        return self.execute_many(sqlbase, key, pool, additional_conditions, select_match, order_by = order_by)

    def delete_many(self, table, key, pool, additional_conditions = [], delete_match = True):
        sqlbase = 'DELETE FROM `{table}`'.format(table = table)

        self.execute_many(sqlbase, key, pool, additional_conditions, delete_match)

    def delete_in(self, table, key, pool, additional_conditions = []):
        self.delete_many(table, key, pool, additional_conditions = additional_conditions, delete_match = True)

    def delete_not_in(self, table, key, pool, additional_conditions = []):
        self.delete_many(table, key, pool, additional_conditions = additional_conditions, delete_match = False)

    def insert_many(self, table, fields, mapping, objects, do_update = True):
        """
        INSERT INTO table (fields) VALUES (mapping(objects)).
        Arguments:
         table: table name.
         fields: name of columns.
         mapping: typically a lambda that takes an element in the objects list and return a tuple corresponding to a row to insert.
         objects: list of objects to insert.
        """

        if len(objects) == 0:
            return

        sqlbase = 'INSERT INTO `{table}` ({fields}) VALUES %s'.format(table = table, fields = ','.join(['`%s`' % f for f in fields]))
        if do_update:
            sqlbase += ' ON DUPLICATE KEY UPDATE ' + ','.join(['`{f}`=VALUES(`{f}`)'.format(f = f) for f in fields])

        template = '(' + ','.join(['%s'] * len(fields)) + ')'
        # template = (%s, %s, ...)

        values = ''
        for obj in objects:
            if mapping is None:
                values += template % MySQLdb.escape(obj, MySQLdb.converters.conversions)
            else:
                values += template % MySQLdb.escape(mapping(obj), MySQLdb.converters.conversions)
            
            # MySQL allows queries up to 1M characters
            if len(values) > config.mysql.max_query_len or obj == objects[-1]:
                self.query(sqlbase % values)

                values = ''

            else:
                values += ','

    def make_snapshot(self, tag):
        snapshot_db = self.db_name() + '_' + tag

        self.query('CREATE DATABASE `{copy}`'.format(copy = snapshot_db))

        tables = self.query('SHOW TABLES')

        for table in tables:
            self.query('CREATE TABLE `{copy}`.`{table}` LIKE `{orig}`.`{table}`'.format(copy = snapshot_db, orig = self.db_name(), table = table))

            self.query('INSERT INTO `{copy}`.`{table}` SELECT * FROM `{orig}`.`{table}`'.format(copy = snapshot_db, orig = self.db_name(), table = table))

        return snapshot_db

    def remove_snapshot(self, tag = '', newer_than = time.time(), older_than = 0):
        if tag:
            self.query('DROP DATABASE ' + self.db_name() + '_' + tag)

        else:
            snapshots = self.list_snapshots(timestamp_only = True)
            for snapshot in snapshots:
                tm = int(time.mktime(time.strptime(snapshot, '%y%m%d%H%M%S')))
                if (newer_than == older_than and tm == newer_than) or \
                        (tm > newer_than and tm < older_than):
                    database = self.db_name() + '_' + snapshot
                    logger.info('Dropping database ' + database)
                    self.query('DROP DATABASE ' + database)

    def list_snapshots(self, timestamp_only):
        databases = self.query('SHOW DATABASES')

        if timestamp_only:
            snapshots = [db.replace(self.db_name() + '_', '') for db in databases if re.match(self.db_name() + '_[0-9]{12}$', db)]
        else:
            snapshots = [db.replace(self.db_name() + '_', '') for db in databases if db.startswith(self.db_name() + '_')]

        return sorted(snapshots, reverse = True)

    def recover_from(self, tag):
        snapshot_name = self.db_name() + '_' + tag

        snapshot_tables = self.query('SHOW TABLES FROM `%s`' % snapshot_name)
        current_tables = self.query('SHOW TABLES')

        for table in snapshot_tables:
            if table not in current_tables:
                self.query('CREATE TABLE `{current}`.`{table}` LIKE `{snapshot}`.`{table}`'.format(current = self.db_name(), snapshot = snapshot_name, table = table))
            else:
                self.query('TRUNCATE TABLE `%s`.`%s`' % (self.db_name(), table))

            self.query('INSERT INTO `{current}`.`{table}` SELECT * FROM `{snapshot}`.`{table}`'.format(current = self.db_name(), snapshot = snapshot_name, table = table))

        for table in current_tables:
            if table not in snapshot_tables:
                self.query('DROP TABLE `%s`.`%s`' % (self.db_name(), table))

    def _execute_in_batches(self, execute, pool):
        """
        Execute the execute function in batches. Pool can be a list or a tuple that defines
        the pool of rows to run execute on.
        """

        if type(pool) is tuple:
            if len(pool) == 2:
                execute('(SELECT `%s` FROM `%s`)' % pool)

            elif len(pool) == 3:
                execute('(SELECT `%s` FROM `%s` WHERE %s)' % pool)

            elif len(pool) == 4:
                # nested pool: the fourth element is the pool argument
                def nested_execute(expr):
                    pool_expr = '(SELECT `%s` FROM `%s` WHERE `%s` IN ' % pool[:3]
                    pool_expr += expr
                    pool_expr += ')'
                    execute(pool_expr)

                self._execute_in_batches(nested_execute, pool[3])

            return

        elif type(pool) is str:
            execute(pool)

            return

        elif type(pool) is list:
            if len(pool) == 0:
                return

            itr = iter(pool)

        elif hasattr(pool, 'next'):
            itr = pool

        # need to repeat in case pool is a long list
        while True:
            pool_expr = ''

            # prepend a '(' instead of ',' for the first item
            delim = '('
            while len(pool_expr) < config.mysql.max_query_len:
                try:
                    itm = itr.next()
                except StopIteration:
                    break

                if type(itm) is str:
                    item = "'%s'" % itm
                else:
                    item = str(itm)

                pool_expr += delim + item
                delim = ','

            if pool_expr == '':
                break

            pool_expr += ')'

            execute(pool_expr)

    def table_exists(self, table):
        return len(self.query('SELECT * FROM `information_schema`.`tables` WHERE `table_schema` = %s AND `table_name` = %s LIMIT 1', self.db_name(), table)) != 0
