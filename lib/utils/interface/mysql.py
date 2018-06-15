import os
import sys
import logging
import time
import re
import multiprocessing
from ConfigParser import ConfigParser

import MySQLdb
import MySQLdb.converters
import MySQLdb.cursors
import MySQLdb.connections

# Fix for some (newer) versions of MySQLdb
from types import TupleType, ListType
MySQLdb.converters.conversions[TupleType] = MySQLdb.converters.escape_sequence
MySQLdb.converters.conversions[ListType] = MySQLdb.converters.escape_sequence

from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)

class MySQL(object):
    """Generic thread-safe MySQL interface (for an interface)."""

    _default_config = Configuration()
    _default_parameters = {'': {}} # {user: config}

    @staticmethod
    def set_default(config):
        MySQL._default_config = Configuration(config)
        MySQL._default_config.pop('params')

        for user, params in config.params.items():
            MySQL._default_parameters[user] = dict(params)
            MySQL._default_parameters[user]['user'] = user

    @staticmethod
    def escape_string(string):
        return MySQLdb.escape_string(string)

    class bare(object):
        """
        Pass bare(string) as column values to bypass formatting in insert_get_id (support will be expanded to other methods).
        """
        def __init__(self, value):
            self.value = value
    
    def __init__(self, config = None):
        config = Configuration(config)

        if 'user' in config:
            user = config.user
        else:
            user = MySQL._default_config.default_user

        try:
            self._connection_parameters = dict(MySQL._default_parameters[user])
        except KeyError:
            self._connection_parameters = {'user': user}

        if 'config_file' in config and 'config_group' in config:
            parser = ConfigParser()
            parser.read(config['config_file'])
            group = config['config_group']
            for ckey, key in [('host', 'host'), ('user', 'user'), ('password', 'passwd'), ('db', 'db')]:
                try:
                    self._connection_parameters[key] = parser.get(group, ckey)
                except:
                    pass

        if 'host' in config:
            self._connection_parameters['host'] = config['host']
        if 'passwd' in config:
            self._connection_parameters['passwd'] = config['passwd']
        if 'db' in config:
            self._connection_parameters['db'] = config['db']

        self._connection = None

        # Avoid interference in case the module is used from multiple threads
        self._connection_lock = multiprocessing.RLock()
        
        # Use with care! A deadlock can occur when another session tries to lock a table used by a session with
        # reuse_connection = True
        if 'reuse_connection' in config:
            self.reuse_connection = config.reuse_connection
        else:
            self.reuse_connection = MySQL._default_config.get('reuse_connection', False)

        # Default 1M characters
        if 'max_query_len' in config:
            self.max_query_len = config.max_query_len
        else:
            self.max_query_len = MySQL._default_config.get('max_query_len', 1000000)

        # Default database for CREATE TEMPORARY TABLE
        if 'scratch_db' in config:
            self.scratch_db = config.scratch_db
        else:
            self.scratch_db = MySQL._default_config.get('scratch_db', '')

        # Row id of the last insertion. Will be nonzero if the table has an auto-increment primary key.
        # **NOTE** While core execution of query() and xquery() are locked and thread-safe, last_insert_id is not.
        # Use insert_and_get_id() in a threaded environment.
        self.last_insert_id = 0

    def db_name(self):
        return self._connection_parameters['db']

    def use_db(self, db):
        self.close()
        if db is None:
            try:
                self._connection_parameters.pop('db')
            except:
                pass
        else:
            self._connection_parameters['db'] = db

    def hostname(self):
        return self.query('SELECT @@hostname')[0]

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def config(self):
        conf = Configuration()
        for key in ['host', 'user', 'passwd', 'db']:
            try:
                conf[key] = self._connection_parameters[key]
            except KeyError:
                pass
        try:
            conf['config_file'] = self._connection_parameters['read_default_file']
        except KeyError:
            pass
        try:
            conf['config_group'] = self._connection_parameters['read_default_group']
        except KeyError:
            pass

        conf['reuse_connection'] = self.reuse_connection
        conf['max_query_len'] = self.max_query_len
        conf['scratch_db'] = self.scratch_db

        return conf

    def get_cursor(self, cursor_cls = MySQLdb.connections.Connection.default_cursor):
        if self._connection is None:
            self._connection = MySQLdb.connect(**self._connection_parameters)

        return self._connection.cursor(cursor_cls)

    def query(self, sql, *args, **kwd):
        """
        Execute an SQL query.
        If the query is an INSERT, return the inserted row id (0 if no insertion happened).
        If the query is an UPDATE, return the number of affected rows.
        If the query is a SELECT, return an array of:
         - tuples if multiple columns are called
         - values if one column is called
        """

        try:
            num_attempts = kwd['retries'] + 1
        except KeyError:
            num_attempts = 10

        try:
            silent = kwd['silent']
        except KeyError:
            silent = False

        self._connection_lock.acquire()

        cursor = None

        try:
            cursor = self.get_cursor()
    
            self.last_insert_id = 0

            if LOG.getEffectiveLevel() == logging.DEBUG:
                if len(args) == 0:
                    LOG.debug(sql)
                else:
                    LOG.debug(sql + ' % ' + str(args))
    
            try:
                for _ in range(num_attempts):
                    try:
                        cursor.execute(sql, args)
                        self._connection.commit()
                        break
                    except MySQLdb.OperationalError as err:
                        if not (self.reuse_connection and err.args[0] == 2006):
                            raise
                            #2006 = MySQL server has gone away
                            #If we are reusing connections, this type of error is to be ignored
                            if not silent:
                                LOG.error(str(sys.exc_info()[1]))

                            last_except = sys.exc_info()[1]

                        # reconnect to server
                        cursor.close()
                        self._connection = None
                        cursor = self.get_cursor()
        
                else: # 10 failures
                    if not silent:
                        LOG.error('Too many OperationalErrors. Last exception:')

                    raise last_except
    
            except:
                if not silent:
                    LOG.error('There was an error executing the following statement:')
                    LOG.error(sql[:10000])
                    LOG.error(sys.exc_info()[1])

                raise
    
            result = cursor.fetchall()
    
            if cursor.description is None:
                if cursor.lastrowid != 0:
                    # insert query on an auto-increment column
                    self.last_insert_id = cursor.lastrowid

                return cursor.rowcount
    
            elif len(result) != 0 and len(result[0]) == 1:
                # single column requested
                return [row[0] for row in result]
    
            else:
                return list(result)

        finally:
            if cursor is not None:
                cursor.close()
    
            if not self.reuse_connection and self._connection is not None:
                self._connection.close()
                self._connection = None

            self._connection_lock.release()

    def insert_get_id(self, table, columns = None, values = None, select = None, db = None, **kwd):
        """
        Auto-form an INSERT statement, execute it under a lock and return the last_insert_id.
        @param table    Table name without reverse-quotes.
        @param columns  If an iterable, column names to insert to. If None, insert filling all columns is assumed.
        @param values   If an iterable, column values to insert. Either values or select must be None.
        @param select   If 
        """

        args = []

        sql = 'INSERT INTO `%s`' % table
        if columns is not None:
            # has to be some iterable
            sql += ' (%s)' % ','.join('`%s`' % c for c in columns)

        if values is not None:
            values_list = []
            for v in values:
                if type(v) is MySQL.bare:
                    values_list.append(v.value)
                else:
                    values_list.append('%s')
                    args.append(v)

            sql += ' VALUES (%s)' % ','.join(values_list)

        elif select is not None:
            sql += ' ' + select

        with self._connection_lock:
            inserted = self.query(sql, *tuple(args), **kwd)
            if type(inserted) is list:
                raise RuntimeError('Non-insert query executed in insert_get_id')
            elif inserted != 1:
                raise RuntimeError('More than one row inserted in insert_get_id')

            return self.last_insert_id

    def xquery(self, sql, *args):
        """
        Execute an SQL query. If the query is an INSERT, return the inserted row id (0 if no insertion happened).
        If the query is a SELECT, return an iterator of:
         - tuples if multiple columns are called
         - values if one column is called
        """

        cursor = None

        try:
            self._connection_lock.acquire()

            cursor = self.get_cursor(MySQLdb.cursors.SSCursor)
    
            self.last_insert_id = 0

            if LOG.getEffectiveLevel() == logging.DEBUG:
                if len(args) == 0:
                    LOG.debug(sql)
                else:
                    LOG.debug(sql + ' % ' + str(args))
    
            try:
                for _ in range(10):
                    try:
                        cursor.execute(sql, args)
                        break
                    except MySQLdb.OperationalError:
                        LOG.error(str(sys.exc_info()[1]))
                        last_except = sys.exc_info()[1]
                        # reconnect to server
                        cursor.close()
                        self._connection = None
                        cursor = self.get_cursor(MySQLdb.cursors.SSCursor)
        
                else: # 10 failures
                    LOG.error('Too many OperationalErrors. Last exception:')
                    raise last_except
    
            except:
                LOG.error('There was an error executing the following statement:')
                LOG.error(sql[:10000])
                LOG.error(sys.exc_info()[1])
                raise
    
            if cursor.description is None:
                raise RuntimeError('xquery cannot be used for non-SELECT statements')
    
            row = cursor.fetchone()
            if row is None:
                # having yield statements below makes this a 0-element iterator
                return
    
            single_column = (len(row) == 1)
    
            while row:
                if single_column:
                    yield row[0]
                else:
                    yield row
    
                row = cursor.fetchone()
    
            return

        finally:
            # only called on exception or return
            if cursor is not None:
                cursor.close()
    
            if not self.reuse_connection and self._connection is not None:
                self._connection.close()
                self._connection = None

            self._connection_lock.release()

    def insert_and_get_id(self, sql, *args, **kwd):
        """
        Thread-safe call for an insertion query to a table with an auto-increment primary key.
        @return (last_insert_id, rowcount)
        """

        with self._connection_lock:
            rowcount = self.query(sql, *args, **kwd)
            return self.last_insert_id, rowcount

    def execute_many(self, sqlbase, key, pool, additional_conditions = [], order_by = ''):
        result = []

        if type(key) is tuple:
            key_str = '(' + ','.join('`%s`' % k for k in key) + ')'
        elif '`' in key or '(' in key:
            key_str = key
        else:
            key_str = '`%s`' % key

        sqlbase += ' WHERE '

        for add in additional_conditions:
            sqlbase += '(%s) AND ' % add

        sqlbase += key_str + ' IN {pool}'

        def execute(pool_expr):
            sql = sqlbase.format(pool = pool_expr)
            if order_by:
                sql += ' ORDER BY ' + order_by

            vals = self.query(sql)
            if type(vals) is list:
                result.extend(vals)

        self._execute_in_batches(execute, pool)

        return result

    def select_many(self, table, fields, key, pool, additional_conditions = [], order_by = ''):
        if type(fields) is str:
            fields = (fields,)

        quoted = []
        for field in fields:
            if '(' in field or '`' in field:
                quoted.append(field)
            else:
                quoted.append('`%s`' % field)

        fields_str = ','.join(quoted)

        sqlbase = 'SELECT {fields} FROM `{table}`'.format(fields = fields_str, table = table)

        return self.execute_many(sqlbase, key, pool, additional_conditions, order_by = order_by)

    def delete_many(self, table, key, pool, additional_conditions = []):
        sqlbase = 'DELETE FROM `{table}`'.format(table = table)

        self.execute_many(sqlbase, key, pool, additional_conditions)

    def insert_many(self, table, fields, mapping, objects, do_update = True, db = '', update_columns = None):
        """
        INSERT INTO table (fields) VALUES (mapping(objects)).
        @param table          Table name.
        @param fields         Name of columns. If None, perform INSERT INTO table VALUES
        @param mapping        Typically a lambda that takes an element in the objects list and return a tuple corresponding to a row to insert.
        @param objects        List or iterator of objects to insert.
        @param do_update      If True, use ON DUPLICATE KEY UPDATE which can be slower than a straight INSERT.
        @param db             DB name.
        @param update_columns Tuple of column names to update when do_update is True. If None, all columns are updated.

        @return  total number of inserted rows.
        """

        try:
            if len(objects) == 0:
                return 0
        except TypeError:
            pass

        # iter() of iterator returns the iterator itself
        itr = iter(objects)

        try:
            # we'll need to have the first element ready below anyway; do it here
            obj = itr.next()
        except StopIteration:
            return 0

        if db == '':
            db = self.db_name()

        sqlbase = 'INSERT INTO `%s`.`%s`' % (db, table)
        if fields:
            sqlbase += ' (%s)' % ','.join('`%s`' % f for f in fields)
        sqlbase += ' VALUES %s'
        if fields and do_update:
            if update_columns is None:
                update_columns = fields

            sqlbase += ' ON DUPLICATE KEY UPDATE ' + ','.join('`{f}`=VALUES(`{f}`)'.format(f = f) for f in update_columns)

        if mapping is None:
            ncol = len(obj)
        else:
            ncol = len(mapping(obj))

        # template = (%s, %s, ...)
        template = '(' + ','.join(['%s'] * ncol) + ')'

        num_inserted = 0

        while True:
            values = ''

            while itr:
                if mapping is None:
                    values += template % MySQLdb.escape(obj, MySQLdb.converters.conversions)
                else:
                    values += template % MySQLdb.escape(mapping(obj), MySQLdb.converters.conversions)
    
                try:
                    obj = itr.next()
                except StopIteration:
                    itr = None
                    break

                # MySQL allows queries up to 1M characters
                if self.max_query_len > 0 and len(values) > self.max_query_len:
                    break

                values += ','

            if values == '':
                break
            
            num_inserted += self.query(sqlbase % values)

        return num_inserted

    def insert_update(self, table, fields, *values):
        placeholders = ', '.join(['%s'] * len(fields))

        sql = 'INSERT INTO `%s` (' % table
        sql += ', '.join('`%s`' % f for f in fields)
        sql += ') VALUES (' + placeholders + ')'
        sql += ' ON DUPLICATE KEY UPDATE '
        sql += ', '.join('`%s`=VALUES(`%s`)' % (f, f) for f in fields)

        return self.query(sql, *values)

    def lock_tables(self, read = [], write = [], **kwd):
        # limitation: can only lock within the same database
        terms = []

        for table in read:
            if type(table) is tuple:
                terms.append('`%s` AS %s READ' % table)
            else:
                terms.append('`%s` READ' % table)

        for table in write:
            if type(table) is tuple:
                terms.append('`%s` AS %s WRITE' % table)
            else:
                terms.append('`%s` WRITE' % table)

        sql = 'LOCK TABLES ' + ', '.join(terms)

        # acquire thread lock so that other threads don't access the database while table locks are on
        self._connection_lock.acquire()

        self.query(sql, **kwd)

    def unlock_tables(self):
        self.query('UNLOCK TABLES')

        self._connection_lock.release()

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

        # case: container or iterator

        try:
            if len(pool) == 0:
                return
        except TypeError:
            pass

        itr = iter(pool)

        try:
            obj = itr.next()
        except StopIteration:
            return

        # need to repeat in case pool is a long list
        while True:
            pool_expr = '('

            while itr:
                pool_expr += MySQLdb.escape(obj, MySQLdb.converters.conversions)

                try:
                    obj = itr.next()
                except StopIteration:
                    itr = None
                    break

                if self.max_query_len > 0 and len(pool_expr) > self.max_query_len:
                    break

                pool_expr += ','

            if pool_expr == '(':
                break

            pool_expr += ')'

            execute(pool_expr)

    def table_exists(self, table, db = ''):
        if not db:
            db = self.db_name()

        return self.query('SELECT COUNT(*) FROM `information_schema`.`tables` WHERE `table_schema` = %s AND `table_name` = %s', db, table)[0] != 0

    def create_tmp_table(self, table, columns, db = ''):
        """
        Create a temporary table. Can be performed with a CREATE TEMPORARY TABLE privilege (not the full CREATE TABLE).
        @param table    Temporary table name
        @param columns  A list or tuple of column definitions (see make_map for an example). If a string (`X`.`Y` or `Y`), then use LIKE syntax to create.
        @param db       Optional DB name (default is scratch_db).
        """
        if not db:
            db = self.scratch_db

        self.drop_tmp_table(table, db = db)

        if type(columns) is str:
            sql = 'CREATE TEMPORARY TABLE `%s`.`%s` LIKE %s' % (db, table, columns)
        else:
            sql = 'CREATE TEMPORARY TABLE `%s`.`%s` (' % (db, table)
            sql += ','.join(columns)
            sql += ') ENGINE=MyISAM DEFAULT CHARSET=latin1'

        self.query(sql)

    def truncate_tmp_table(self, table, db = ''):
        if not db:
            db = self.scratch_db

        if self.table_exists(table, db):
            self.query('TRUNCATE TABLE `%s`.`%s`' % (db, table))

    def drop_tmp_table(self, table, db = ''):
        if not db:
            db = self.scratch_db

        if self.table_exists(table, db):
            self.query('DROP TABLE `%s`.`%s`' % (db, table))

    def make_map(self, table, objects, object_id_map = None, id_object_map = None, key = None, tmp_join = False):
        objitr = iter(objects)

        if tmp_join:
            tmp_table = table + '_map'
            columns = ['`name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL', 'PRIMARY KEY (`name`)']
            self.create_tmp_table(tmp_table, columns)

            # need to create a list first because objects can already be an iterator and iterators can iterate only once
            objlist = list(objitr)
            objitr = iter(objlist)

            if key is None:
                self.insert_many(tmp_table, ('name',), lambda obj: (obj.name,), objlist, db = self.scratch_db)
            else:
                self.insert_many(tmp_table, ('name',), lambda obj: (key(obj),), objlist, db = self.scratch_db)

            name_to_id = dict(self.xquery('SELECT t1.`name`, t1.`id` FROM `%s` AS t1 INNER JOIN `%s`.`%s` AS t2 ON t2.`name` = t1.`name`' % (table, self.scratch_db, tmp_table)))

            self.drop_tmp_table(tmp_table)

        else:
            name_to_id = dict(self.xquery('SELECT `name`, `id` FROM `%s`' % table))

        num_obj = 0
        for obj in objitr:
            num_obj += 1
            try:
                if key is None:
                    obj_id = name_to_id[obj.name]
                else:
                    obj_id = name_to_id[key(obj)]
            except KeyError:
                continue

            if object_id_map is not None:
                object_id_map[obj] = obj_id
            if id_object_map is not None:
                id_object_map[obj_id] = obj

        LOG.debug('make_map %s (%d) obejcts', table, num_obj)
