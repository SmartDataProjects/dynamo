import os
import re
import random
import time
import threading
import Queue
import hashlib
import logging
import shutil
import smtplib
import socket
from email.mime.text import MIMEText

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

        accept_thread = threading.Thread(target = self._accept_applications)
        accept_thread.daemon = True
        accept_thread.start()

        scheduler_thread = threading.Thread(target = self._scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()

        self._running = True

    def stop(self):
        """Stop the server. Applications should have all terminated by the time this function is called."""

        self._running = False
        self._stop_accepting()

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

    def _poll_app(self, app_id):
        app = self._get_app(app_id)

        if app is None:
            return False, 'Unknown appid %d' % app_id

        app['status'] = ServerManager.application_status_name(app['status'])
        return True, app

    def _kill_app(self, app_id):
        app = self._get_app(app_id)

        if app is None:
            return False, 'Unknown appid %d' % app_id

        if app['status'] in (ServerManager.APP_NEW, ServerManager.APP_RUN):
            self.dynamo_server.manager.master.update_application(app_id, status = ServerManager.APP_KILLED)
            return True, {'result': 'success', 'detail': 'Task aborted.'}
        else:
            return True, {'result': 'noaction', 'detail': 'Task already completed with status %s (exit code %s).' % \
                          (ServerManager.application_status_name(app['status']), app['exit_code'])}

    def _get_app(self, app_id):
        apps = self.dynamo_server.manager.master.get_applications(app_id = app_id)
        if len(apps) == 0:
            return None
        else:
            return apps[0]

    def _schedule_app(self, app_data):
        """
        Call schedule_application on the master server. If mode == 'synch', create a communication
        queue and register it under synch_app_queues. The server should then wait on this queue
        before starting the application.
        """

        app_data = dict(app_data)

        # schedule the app on master
        if 'exec_path' in app_data:
            try:
                shutil.copyfile(app_data['exec_path'], workarea + '/exec.py')
            except Exception as exc:
                return False, 'Could not copy executable %s to %s (%s)' % (app_data['exec_path'], workarea, str(exc))

            app_data.pop('exec_path')

        elif 'exec' in app_data:
            with open(workarea + '/exec.py', 'w') as out:
                out.write(app_data['exec'])
                
            app_data.pop('exec')

        mode = app_data.pop('mode')

        with self.notify_lock:
            keys = set(app_data.keys())
            args = set(['title', 'path', 'args', 'user', 'host', 'write_request'])
            if len(keys - args) != 0:
                return False, 'Extra parameter(s): %s' % (str(list(keys - args)))
            if len(args - keys) != 0:
                return False, 'Missing parameter(s): %s' % (str(list(args - keys)))

            app_id = self.dynamo_server.manager.master.schedule_application(**app_data)
            if mode == 'synch':
                self.synch_app_queues[app_id] = Queue.Queue()

        if mode == 'synch':
            msg = self.wait_synch_app_queue(app_id)

            if msg['status'] != ServerManager.APP_RUN:
                # this app is not going to run
                return False, 'Application status: %s.' % ServerManager.application_status_name(msg['status'])

            return True, {'appid': app_id, 'path': msg['path'], 'pid': msg['pid']} # msg['path'] should be == workarea
        else:
            return True, {'appid': app_id, 'path': workarea}

    def _add_sequences(self, path):
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

                with open(work_dir + '/log.out', 'w'):
                    pass
            
                with open(work_dir + '/log.err', 'w'):
                    pass

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
            sql += '`id` INTEGER PRIMARY KEY,'
            sql += '`line` INTEGER NOT NULL,'
            sql += '`command` TINYINT NOT NULL,'
            sql += '`title` TEXT DEFAULT NULL,'
            sql += '`arguments` TEXT DEFAULT NULL,'
            sql += '`criticality` TINYINT DEFAULT NULL,'
            sql += '`write_request` TINYINT DEFAULT NULL'
            sql += '`app_id` INTEGER DEFAULT NULL'
            sql += ')'
            db.execute(sql)

            for iline, action in enumerate(sequence):
                if action[0] == AppServer.EXECUTE:
                    sql = 'INSERT INTO `sequence` (`line`, `command`, `title`, `arguments`, `criticality`, `write_request`)'
                    sql += ' VALUES (?, ?, ?, ?, ?, ?)'
                    cursor.execute(sql, (iline,) + action)

                elif action[1] == AppServer.WAIT:
                    sql = 'INSERT INTO `sequence` (`line`, `command`, `title`)'
                    sql += ' VALUES (?, ?)'
                    cursor.execute(sql, (iline, action[0], str(action[1])))

                elif action[1] == AppServer.TERMINATE:
                    sql = 'INSERT INTO `sequence` (`line`, `command`)'
                    sql += ' VALUES (?)'
                    cursor.execute(sql, (iline, action[0]))

            db.commit()
            db.close()

            with open(work_dir + '/state', 'w') as out:
                out.write('disabled\n')

        return True, ''

    def _delete_sequence(self, name):
        if not os.path.exists(self.scheduler_base + '/' + name):
            return False, 'Sequence %s does not exist.' % name

        self._stop_seqeunce(name)

        try:
            shutil.rmtree(self.scheduler_base + '/' + name)
        except:
            return False, 'Failed to delete sequence %s.' % name

        return True, ''

    def _start_sequence(self, name):
        try:
            with open(self.scheduler_base + '/' + name + '/state', 'w') as out:
                out.write('enabled\n')
        except:
            return False, 'Failed to start sequence %s.' % name

        return True, ''

    def _stop_sequence(self, name):
        work_dir = self.scheduler_base + '/' + name

        try:
            with open(work_dir + '/state', 'w') as out:
                out.write('disabled\n')
        except:
            return False, 'Failed to stop sequence %s.' % name

        # kill all running applications
        try:
            db = sqlite3.connect(work_dir + '/sequence.db')
            cursor = db.cursor()
            cursor.execute('SELECT `app_id` FROM `sequence` WHERE `app_id` IS NOT NULL')
            for row in cursor.fetchall():
                app = self._get_app(row[0])
                if app is not None and app['status'] not in (ServerManager.APP_DONE, ServerManager.APP_FAILED, ServerManager.APP_KILLED):
                    self.dynamo_server.manager.master.update_application(row[0], status = ServerManager.APP_KILLED)

        return True, ''

    def _scheduler(self):
        """
        A function to be run as a thread. Rotates through the scheduler sequence directories and execute whatever is up next.
        Perhaps we want an independent logger for this thread
        """

        while True:
            for sequence_name in os.listdir(self.scheduler_base):
                work_dir = self.scheduler_base + '/' + sequence_name
                
                try:
                    with open(work_dir + '/state') as source:
                        state = source.read().strip()
                except:
                    LOG.error('[Scheduler] %s/state missing', work_dir)
                    continue

                if state != 'enabled':
                    continue

                db = sqlite3.connect(work_dir + '/sequence.db')
                cursor = db.cursor()
                try:
                    cursor.execute('SELECT `line`, `command`, `title`, `arguments`, `criticality`, `write_request`, `app_id` FROM `sequence` ORDER BY `id` LIMIT 1')
                    row = cursor.fetchone()
                    if row is None:
                        raise Exception()
                except:
                    LOG.error('[Scheduler] Failed to fetch the current command for sequence %s.', sequence_name)
                    continue

                db.close()

                iline, command, title, arguments, criticality, write_request, app_id = row

                if command == AppServer.EXECUTE:
                    # poll the app_id
                    app = self._get_app(app_id)

                    if app is None:
                        LOG.error('[Scheduler] Application %s in sequence %s disappeared.', title, sequence_name)
                        self._schedule_from_sequence(sequence_name, iline)
                    else:
                        if app['status'] in (ServerManager.APP_NEW, ServerManager.APP_ASSIGNED, ServerManager.APP_RUN):
                            continue
                        else:
                            try:
                                with open(work_dir + '/log.out', 'a') as out:
                                    out.write('\n')
                            except:
                                pass
            
                            try:
                                with open(work_dir + '/log.err', 'a') as out:
                                    out.write('\n')
                            except:
                                pass

                            if app['status'] == ServerManager.APP_DONE:
                                LOG.info('[Scheduler] Application %s in sequence %s completed.', title, sequence_name)
                                self._schedule_from_sequence(sequence_name, iline + 1)

                            else:
                                LOG.warning('[Scheduler] Application %s in sequence %s terminated with status %s.', title, sequence_name, ServerManager.application_status_name(app['status']))
                                if criticality != AppServer.PASS:
                                    self._send_failure_notice(sequence_name, app)

                                if criticality == AppServer.REPEAT_SEQ:
                                    LOG.warning('[Scheduler] Restarting sequence %s.', sequence_name)
                                    self._schedule_from_sequence(sequence_name, 0)
                                elif criticality == AppServer.REPEAT_LINE:
                                    LOG.warning('[Scheduler] Restarting application %s of sequence.', title, sequence_name)
                                    self._schedule_from_sequence(sequence_name, iline)

                elif command == AppServer.WAIT:
                    # title is the number of seconds expressed in a decimal string
                    # arguments is set to the unix timestamp (string) until when the sequence should wait
                    wait_until = int(arguments)
                    if time.time() < wait_until:
                        continue
                    else:
                        self._schedule_from_sequence(sequence_name, iline + 1)

            # all sequences processed; now sleep for 10 seconds
            time.sleep(10)

    def _schedule_from_sequence(self, sequence_name, to_line):
        work_dir = self.scheduler_base + '/' + sequence_name

        db = None
        try:
            db = sqlite3.connect(work_dir + '/sequence.db')
            cursor = db.cursor()
    
            cursor.execute('SELECT COUNT(*) FROM `sequence` WHERE `command` = ?', (AppServer.TERMINATE,))
            terminated = cursor.fetchone()[0]

            cursor.execute('SELECT MAX(`line`) FROM `sequence`')
            max_line = cursor.fetchone()[0]

            to_line %= max_line
    
            cursor.execute('SELECT `id`, `line`, `command`, `title`, `arguments`, `criticality`, `write_request` FROM `sequence` ORDER BY `id`')
            for row in cursor.fetchall():
                if row[1] == to_line:
                    break

                cursor.execute('DELETE FROM `sequence` WHERE `id` = ?', (row[0],))
    
                if not terminated:
                    # this sequence is an infinite loop; move the entry to the bottom
                    cursor.execute('INSERT INTO `sequence` (`line`, `command`, `title`, `arguments`, `criticality`, `write_request`) VALUES (?, ?, ?, ?, ?, ?)', row)

            else:
                # looped through??
                LOG.error('Could not find line %d in sequence %s', to_line, sequence_name)
                return
                
            sid, iline, command, title, arguments, criticality, write_request = row

            if command == AppServer.EXECUTE:
                self.dynamo_server.manager.master.schedule_application(title, work_dir + '/' + title, arguments, self.dynamo_server.user, socket.gethostname(), (write_request != 0))

            elif command == AppServer.WAIT:
                time_wait = int(title)
                cursor.execute('UPDATE `sequence` SET `arguments` = ? WHERE `id` = ?', (int(time.time()) + time_wait, sid))

            elif command == AppServer.TERMINATE:
                self._stop_sequence(sequence_name)

        except:
            LOG.error('Failed to schedule line %d of sequence %s.', to_line, sequence_name)

        finally:
            if db is not None:
                try:
                    db.commit()
                    db.close()
                except:
                    pass

    def _send_failure_notice(self, sequence_name, app):
        text = 'Message from dynamo-scheduled@%s:\n' % socket.gethostname()
        text += 'Application %s of sequence %s failed.\n' % (app['title'], sequence_name)
        text += 'Details:\n'
        for key in sorted(app.keys()):
            text += ' %s = %s\n' % (key, app[key])

        msg = MIMEText(text)
        msg['Subject'] = '[Dynamo Scheduler] %s/%s failed' % (sequence_name, app['title'])
        msg['From'] = 'dynamo@' + socket.gethostname()
        msg['To'] = config.notification_recipient

        mailserv = smtplib.SMTP('localhost')
        mailserv.sendmail(msg['From'], [msg['To']], msg.as_string())
        mailserv.quit()
