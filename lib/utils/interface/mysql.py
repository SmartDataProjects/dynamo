import os
import sys
import logging
import time
import re
import threading

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

    _default_parameters = {}

    @staticmethod
    def set_default(config):
        MySQL._default_parameters = dict(config)

    @staticmethod
    def escape_string(string):
        return MySQLdb.escape_string(string)
    
    def __init__(self, config):
        config = Configuration(config)

        self._connection_parameters = dict(MySQL._default_parameters)

        if 'config_file' in config and 'config_group' in config:
            # Check file exists and readable
            with open(config['config_file']):
                pass

            self._connection_parameters['read_default_file'] = config['config_file']
            self._connection_parameters['read_default_group'] = config['config_group']
        if 'host' in config:
            self._connection_parameters['host'] = config['host']
        if 'user' in config:
            self._connection_parameters['user'] = config['user']
        if 'passwd' in config:
            self._connection_parameters['passwd'] = config['passwd']
        if 'db' in config:
            self._connection_parameters['db'] = config['db']

        self._connection = None

        # Avoid interference in case the module is used from multiple threads
        self._connection_lock = threading.RLock()
        
        # Use with care! A deadlock can occur when another session tries to lock a table used by a session with
        # reuse_connection = True
        self.reuse_connection = config.get('reuse_connection', False)

        # Default 1M characters
        self.max_query_len = config.get('max_query_len', 1000000)

        # Row id of the last insertion. Will be nonzero if the table has an auto-increment primary key.
        # **NOTE** While core execution of query() and xquery() are locked and thread-safe, last_insert_id is not.
        # Use insert_and_get_id() in a threaded environment.
        self.last_insert_id = 0

    def db_name(self):
        return self._connection_parameters['db']

    def use_db(self, db):
        self.close()
        self._connection_parameters['db'] = db

    def hostname(self):
        cursor = self.get_cursor()
        cursor.execute('SELECT @@hostname')
        result = cursor.fetchall()
        cursor.close()
        return result[0][0]

    def close(self):
        if self._connection is not None:
            self._connection.close()

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

    def insert_many(self, table, fields, mapping, objects, do_update = True, db = ''):
        """
        INSERT INTO table (fields) VALUES (mapping(objects)).
        @param table         Table name.
        @param fields        Name of columns.
        @param mapping       Typically a lambda that takes an element in the objects list and return a tuple corresponding to a row to insert.
        @param objects       List or iterator of objects to insert.
        @param do_update     If True, use ON DUPLICATE KEY UPDATE which can be slower than a straight INSERT.
        @param db            DB name.

        @return  total number of inserted rows.
        """

        try:
            if len(objects) == 0:
                return
        except TypeError:
            pass

        # iter() of iterator returns the iterator itself
        itr = iter(objects)

        try:
            # we'll need to have the first element ready below anyway; do it here
            obj = itr.next()
        except StopIteration:
            return

        if db == '':
            db = self.db_name()

        sqlbase = 'INSERT INTO `{db}`.`{table}` ({fields}) VALUES %s'.format(db = db, table = table, fields = ','.join(['`%s`' % f for f in fields]))
        if do_update:
            sqlbase += ' ON DUPLICATE KEY UPDATE ' + ','.join(['`{f}`=VALUES(`{f}`)'.format(f = f) for f in fields])

        template = '(' + ','.join(['%s'] * len(fields)) + ')'
        # template = (%s, %s, ...)

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
                    LOG.info('Dropping database ' + database)
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
                if type(obj) is str:
                    pool_expr += "'%s'" % obj
                else:
                    pool_expr += str(obj)

                try:
                    obj = itr.next()
                except StopIteration:
                    itr = None
                    break

                if self.max_query_len > 0 and len(pool_expr) < self.max_query_len:
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
        if not db:
            db = self.db_name()

        tmp_db = db + '_tmp'
        tmp_table = '%s_tmp' % table

        self.drop_tmp_table(table, db = db)

        sql = 'CREATE TEMPORARY TABLE `%s`.`%s` (' % (tmp_db, tmp_table)
        sql += ','.join(columns)
        sql += ') ENGINE=MyISAM DEFAULT CHARSET=latin1'

        self.query(sql)

        return tmp_db, tmp_table

    def drop_tmp_table(self, table, db = ''):
        if not db:
            db = self.db_name()

        tmp_db = db + '_tmp'
        tmp_table = '%s_tmp' % table

        if self.table_exists(tmp_table, db = tmp_db):
            self.query('DROP TEMPORARY TABLE `%s`.`%s`' % (tmp_db, tmp_table))

    def make_map(self, table, objects, object_id_map = None, id_object_map = None, key = None, tmp_join = False):
        objitr = iter(objects)

        if tmp_join:
            columns = ['`name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL', 'PRIMARY KEY (`name`)']
            tmp_db, tmp_table = self.create_tmp_table(table + '_map', columns)

            # need to create a list first because objects can already be an iterator and iterators can iterate only once
            objlist = list(objitr)
            objitr = iter(objlist)

            if key is None:
                self.insert_many(tmp_table, ('name',), lambda obj: (obj.name,), objlist, db = tmp_db)
            else:
                self.insert_many(tmp_table, ('name',), lambda obj: (key(obj),), objlist, db = tmp_db)

            name_to_id = dict(self.xquery('SELECT t1.`name`, t1.`id` FROM `%s` AS t1 INNER JOIN `%s`.`%s` AS t2 ON t2.`name` = t1.`name`' % (table, tmp_db, tmp_table)))

            self.drop_tmp_table(table)

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
