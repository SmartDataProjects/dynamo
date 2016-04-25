"""
Generic MySQL interface (for an interface).
"""

import MySQLdb
import sys
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

class MySQL(object):
    
    def __init__(self, host = '', user = '', passwd = '', config_file = '', config_group = '', db = ''):
        if default_file:
            self._connection = MySQLdb.connect(read_default_file = default_file, read_default_group = config_group, db = db)
        else:
            self._connection = MySQLdb.connect(host = host, user = user, passwd = passwd, db = db)

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

        cursor.execute(sql, args)

        result = cursor.fetchall()

        if cursor.description is None:
            # insert query
            return cursor.lastrowid

        elif len(result) != 0 and len(result[0]) == 1:
            # single column requested
            return [row[0] for row in result]

        else:
            return list(result)

    def select_many(self, table, fields, key, pool):
        result = []

        def execute(pool_expr):
            condition = '`%s` IN ' % key
            condition += pool_expr

            conditions = [condition] + additional_conditions
    
            if type(fields) is str:
                fields_str = '`%s`' % fields
            else:
                fields_str = ','.join(['`%s`' % f for f in fields])

            sql = 'SELECT %s FROM `%s` WHERE ' % (fields_str, table)
            sql += ' AND '.join(conditions)
            
            result += self.query(sql)

        self._execute_in_batches(execute, pool)

        return result

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

        # determine which columns are string types
        templates = []
        for ifield, val in enumerate(mapping(objects[0])):
            if type(val) is str:
                templates.append('\'{%d}\'' % ifield)
            else:
                templates.append('{%d}' % ifield)

        template = '(%s)' % (','.join(templates))
        # in the end template string looks like "({0},'{1}',{2})"
        # template.format(*tuple) will fill in the placeholders in order

        cursor = self._connection.cursor()

        values = ''
        for obj in objects:
            values += template.format(*mapping(obj))
            
            # MySQL allows queries up to 1M characters
            if len(values) > config.mysql.max_query_len or obj == objects[-1]:
                logger.debug(sqlbase % values)
                try:
                    cursor.execute(sqlbase % values)
                except:
                    print 'There was an error executing the following statement:'
                    print (sqlbase % values)[:10000]
                    print sys.exc_info()[1]

                values = ''

            else:
                values += ','

    def delete_many(self, table, key, pool, additional_conditions = [], delete_match = True):
        def execute(pool_expr):
            condition = '`%s`' % key
            if delete_match:
                condition += ' IN '
            else:
                condition += ' NOT IN '

            condition += pool_expr

            conditions = [condition] + additional_conditions
    
            sql = 'DELETE FROM `%s` WHERE ' % table
            sql += ' AND '.join(conditions)
            
            self.query(sql)

        self._execute_in_batches(execute, pool)

    def delete_in(self, table, key, pool, additional_conditions = []):
        self.delete_many(table, key, pool, additional_conditions = additional_conditions, delete_match = True)

    def delete_not_in(self, table, key, pool, additional_conditions = []):
        self.delete_many(table, key, pool, additional_conditions = additional_conditions, delete_match = False)

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

        elif type(pool) is list:
            # need to repeat in case pool is a long list
            iP = 0
            while iP < len(pool):
                pool_expr = '('

                query_len = len(pool_expr)
                items = []
                while query_len < config.mysql.max_query_len and iP < len(pool):
                    item = str(pool[iP])
                    query_len += len(item)
                    items.append(item)
                    iP += 1

                pool_expr += ','.join(items)
                pool_expr += ')'
    
                execute(pool_expr)

        elif type(pool) is str:
            execute(pool)
