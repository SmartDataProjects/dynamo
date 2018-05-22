import os
import re
import random
import threading
import Queue
import hashlib
import logging
import shutil

LOG = logging.getLogger(__name__)

class AppServer(object):
    """Base class for application server."""

    # commands
    EXECUTE, WAIT, TERMINATE = range(3)
    # criticality
    REPEAT_SEQ, REPEAT_LINE, PASS = range(3)

    @staticmethod
    def get_instance(module, dynamo_server, config):
        import dynamo.core.components.impl as impl
        cls = getattr(impl, module)
        if not issubclass(cls, AppServer):
            raise RuntimeError('%s is not a subclass of AppServer' % module)

        return cls(dynamo_server, config)

    def __init__(self, dynamo_server, config):
        self.dynamo_server = dynamo_server

        # Base directory for application work areas
        # Caution: everything under this directory is subject to automatic cleanup by the Dynamo server
        self.workarea_base = config.workarea_base

        # Base directory for scheduled application sequences
        self.scheduler_base = config.scheduler_base

        ## Queues synchronous applications will wait on. {app_id: Queue}
        self.synch_app_queues = {}
        ## notify_synch_lock can be called from the DynamoServer immediately
        ## after the application is scheduled. Need a lock to make sure we
        ## register them first.
        self.notify_lock = threading.Lock()

        self._running = False

    def start(self):
        """Start a daemon thread that runs the accept loop and return."""

        th = threading.Thread(target = self._accept_applications)
        th.daemon = True
        th.start()

        self._running = True

    def stop(self):
        """Stop the server. Applications should have all terminated by the time this function is called."""

        self._running = False

        for dname in os.listdir(self.scheduler_base):
            shutil.rmtree(self.scheduler_base + '/' + dname)

        self._do_stop()

    def notify_synch_app(self, app_id, data):
        """
        Notify synchronous app.
        @param app_id  App id (key in synch_app_queues)
        @param data    Dictionary passed to thread waiting to start a synchronous app.
        """
        with self.notify_lock:
            try:
                self.synch_app_queues[app_id].put(data)
            except KeyError:
                pass

    def wait_synch_app_queue(self, app_id):
        """
        Wait on queue and return the data put in the queue.
        @param app_id  App id (key in synch_app_queues)
        """
        return self.synch_app_queues[app_id].get()

    def remove_synch_app_queue(self, app_id):
        self.synch_app_queues.pop(app_id)

    def _accept_applications(self):
        """Infinite loop to serve incoming connections."""

        raise NotImplementedError('_accept_applications')

    def _make_workarea(self):
        """
        Make a work area under spool with a random 64-bit hex as the name. This can be a static function.
        """

        while True:
            workarea = '{0}/{1:016x}'.format(self.workarea_base, random.randint(0, 0xffffffffffffffff)) # zero-padded 16-char length hex
            try:
                os.makedirs(workarea)
            except OSError:
                if not os.path.exists(workarea):
                    return '' # failed to create the work area
                else:
                    # remarkably, the directory existed
                    continue

            return workarea

    def _schedule_app(self, mode, **app_data):
        """
        Call schedule_application on the master server. If mode == 'synch', create a communication
        queue and register it under synch_app_queues. The server should then wait on this queue
        before starting the application.
        """
        with self.notify_lock:
            app_id = self.dynamo_server.manager.master.schedule_application(**app_data)
            if mode == 'synch':
                self.synch_app_queues[app_id] = Queue.Queue()

        return app_id

    def _parse_sequence_defs(self, path):
        """
        Parse a sequence definition file and create an sqlite3 database for each sequence.
        @param path   Name of the file containing one or more sequence definitions.
        """

        LOG.info('Parsing sequence definition %s', path)

        scheduler = self.dynamo_server.manager.master

        app_paths = {} # {title: exec path}
        authorized_applications = set() # set of titles
        sequences = {} # {name: sequence}
        sequence = None

        with open(path) as source:
            iline = -1
            for line in source:
                iline += 1
        
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
        
                LOG.debug(line)
        
                # Application definitions
                # {title} = path  ...  This application cannot write
                # <title> = path  ...  This application can be used to write
                matches = re.match('({\S+}|<\S+>)\s*=\s*(\S+)', line)
                if matches:
                    enclosed_title = matches.group(1)
                    title = enclosed_title[1:-1]
                    write_enabled = (enclosed_title[0] == '<')
                    application = matches.group(2)
        
                    # Replace environment variables
                    matches = re.findall('\$\(([^\)]+)\)', application)
                    for match in matches:
                        application = application.replace('$(%s)' % match, os.environ[match])
    
                    with open(application) as source:
                        checksum = hashlib.md5(source.read()).hexdigest()
        
                    if write_enabled and not scheduler.check_application_auth(title, self.dynamo_server.user, checksum):
                        return False, 'Application %s (%s) is not authorized for server write operation (line %d).' % (title, application, iline)
        
                    LOG.debug('Define application %s = %s (write enabled: %d) (line %d)', title, application, write_enabled, iline)
        
                    app_paths[title] = application
                    if write_enabled:
                        authorized_applications.add(title)
                    
                    continue
        
                # Sequence definitions
                # [SEQUENCE title]
                matches = re.match('\[SEQUENCE\s(\S+)\]', line)
                if matches:
                    # Sequence header
                    LOG.debug('New sequence %s (line %d)', matches.group(1), iline)
                    sequence = sequences[matches.group(1)] = []
                    continue

                # If the line is not an application or sequence definition, there needs to be an open sequence
                if sequence is None:
                    return False, 'Invalid line "%s" before sequence definition is given.' % line
        
                # Sequence application step definitions
                # {title} options  ...  Read-only execution
                # <title> options  ...  Write-request execution
                matches = re.match('(\^|\&|\|) +({\S+}|<\S+>)\s*(.*)', line)
                if matches:
                    if matches.group(1) == '^':
                        criticality = AppServer.REPEAT_SEQ
                    elif matches.group(1) == '&':
                        criticality = AppServer.REPEAT_LINE
                    else:
                        criticality = AppServer.PASS
        
                    enclosed_title = matches.group(2)
                    title = enclosed_title[1:-1]
                    write_request = (enclosed_title[0] == '<')
                    arguments = matches.group(3)
        
                    if write_request and title not in authorized_applications:
                        return False, 'Application %s is not write-enabled (line %d).' % iline
        
                    # Replace environment variables
                    matches = re.findall('\$\(([^\)]+)\)', arguments)
                    for match in matches:
                        arguments = arguments.replace('$(%s)' % match, os.environ[match])
        
                    LOG.debug('Execute %s %s (line %d)', title, arguments, iline)
        
                    sequence.append((AppServer.EXECUTE, title, arguments, criticality, write_request))
                    continue
        
                matches = re.match('WAIT\s+(.*)', line)
                if matches:
                    try:
                        sleep_time = eval(matches.group(1))
                        if type(sleep_time) not in [int, float]:
                            raise RuntimeError()
                    except:
                        return False, 'Wait time %s is not a numerical expression (line %d).' % (matches.group(1), iline)
        
                    LOG.debug('Wait for %d seconds (line %d)', sleep_time, iline)
                    sequence.append((AppServer.WAIT, sleep_time))
                    continue
        
                if line == 'TERMINATE':
                    sequence.append([AppServer.TERMINATE])

        for name in sequences.keys():
            if os.path.exists(self.scheduler_base + '/' + name):
                return False, 'Sequence %s already exists.' % name

        for name, sequence in sequences.items():
            work_dir = self.scheduler_base + '/' + name

            try:
                os.makedirs(work_dir)
                os.chmod(work_dir, 0777)
            
                with open(work_dir + '/log.out', 'w'):
                    pass
            
                os.chmod(work_dir + '/log.out', 0666)
            
                with open(work_dir + '/log.err', 'w'):
                    pass
            
                os.chmod(work_dir + '/log.err', 0666)
            except:
                try:
                    shutil.rmtree(work_dir)
                except:
                    pass

                return False, 'Failed to set up %s.' % name
        
            for action in sequence:
                if action[0] == EXECUTE:
                    title = action[1]
        
                    path = '%s/%s' % (work_dir, title)
                    if os.path.exists(path):
                        # This application is used multiple times in the sequence
                        continue
        
                    app_path = app_paths[title]
        
                    os.makedirs(path)
                    shutil.copy(app_path, path + '/exec.py')
        
                    # Make symlinks from subdir/_std(out|err) to log.(out|err)
                    os.symlink(work_dir + '/log.out', path + '/_stdout')
                    os.symlink(work_dir + '/log.err', path + '/_stderr')

            db = sqlite3.connect(work_dir + '/sequence.db')
            cursor = db.cursor()

            sql = 'CREATE TABLE `sequence` ('
            sql += '`id` INTEGERY PRIMARY KEY,'
            sql += '`command` TINYINT NOT NULL,'
            sql += '`title` TEXT NOT NULL,'
            sql += '`arguments` TEXT NOT NULL,'
            sql += '`criticality` TINYINT NOT NULL,'
            sql += '`write_request` TINYINT NOT NULL'
            sql += ')'
            db.execute(sql)

            for action in sequence:
                if action[0] == AppServer.EXECUTE:
                    sql = 'INSERT INTO `sequence` (`command`, `title`, `arguments`, `criticality`, `write_request`)'
                    sql += ' VALUES (?, ?, ?, ?, ?)'
                    cursor.execute(sql, action)

                elif action[1] == AppServer.WAIT:
                    sql = 'INSERT INTO `sequence` (`command`, `title`)'
                    sql += ' VALUES (?, ?)'
                    cursor.execute(sql, (action[0], str(action[1])))

                elif action[1] == AppServer.TERMINATE:
                    sql = 'INSERT INTO `sequence` (`command`)'
                    sql += ' VALUES (?)'
                    cursor.execute(sql, (action[0],))

            db.commit()
            db.close()

            os.chmod(work_dir + '/sequence.db', 0666)
