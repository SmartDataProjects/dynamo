import os
import sys
import socket
import time
import thread
import threading
import multiprocessing
import tempfile
import shutil
import code
import ssl
import json
import traceback
import logging
import subprocess

from dynamo.core.components.appserver import AppServer
from dynamo.core.components.appmanager import AppManager
import dynamo.core.serverutils as serverutils
from dynamo.dataformat import ConfigurationError

SERVER_PORT = 39626
DN_TRANSLATION = {
    'commonName': 'CN',
    'localityName': 'L',
    'stateOrProvinceName': 'ST',
    'organizationName': 'O',
    'organizationalUnitName': 'OU',
    'countryName': 'C',
    'streetAddress': 'STREET',
    'domainComponent': 'DC',
    'userId': 'UID'
}

# OpenSSL cannot authenticate with certificate proxies without this environment variable
os.environ['OPENSSL_ALLOW_PROXY_CERTS'] = '1'

LOG = logging.getLogger(__name__)

class SocketIO(object):
    def __init__(self, conn, addr):
        self.conn = conn
        self.host = addr[0]
        self.port = addr[1]

    def send(self, status, content = ''):
        """
        Send a JSON with format {'status': status, 'content': content}. If status is not OK, log
        the content.
        """

        if status != 'OK':
            LOG.error('Response to %s:%d: %s', self.host, self.port, content)

        bytes = json.dumps({'status': status, 'content': content})
        try:
            self.conn.sendall('%d %s' % (len(bytes), bytes))
        except:
            pass

    def recv(self):
        """
        Read a message possibly split in multiple transmissions. The message must have a form or a decimal
        number corresponding to the length of the content, followed by a space, and the content in JSON.
        """

        data = ''
        while True:
            try:
                bytes = self.conn.recv(2048)
            except socket.error:
                break
            if not bytes:
                break

            if not data:
                # first communication
                length, _, bytes = bytes.partition(' ')
                length = int(length)

            data += bytes

            if len(data) >= length:
                # really should be == but to be prepared for malfunction
                break

        try:
            return json.loads(data)
        except:
            self.send('failed', 'Ill-formatted data')
            raise RuntimeError()

def tail_follow(source_path, stream, stop_reading):
    ## tail -f emulation
    while True:
        if os.path.exists(source_path):
            break

        if stop_reading.is_set():
            break

        time.sleep(0.5)

    try:
        with open(source_path) as source:
            while True:
                pos = source.tell()
                line = source.readline()
                if not line:
                    if stop_reading.is_set():
                        return

                    source.seek(pos)
                    time.sleep(0.5)
                else:
                    stream.sendall(line)
    except:
        pass


class SocketAppServer(AppServer):
    """
    Sub-server owned by the main Dynamo server to serve application requests.
    """

    def __init__(self, dynamo_server, config):
        AppServer.__init__(self, dynamo_server, config)

        try:
            port = int(os.environ['DYNAMO_SERVER_PORT'])
        except:
            port = SERVER_PORT

        self._port = port

        try:
            self._context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        except AttributeError:
            self._context = None

            # python 2.6
            if os.path.isdir(config.capath):
                raise ConfigurationError('AppServer configuration parameter "capath" must point to a single file.')

            self._certfile = config.certfile
            with open(config.keyfile) as source:
                self._keyfile_content = source.read()
            self._capath = config.capath
        else:
            # python 2.7
            self._context.load_cert_chain(config.certfile, keyfile = config.keyfile)
            if os.path.isdir(config.capath):
                self._context.load_verify_locations(capath = config.capath)
            else:
                self._context.load_verify_locations(cafile = config.capath)
            self._context.verify_mode = ssl.CERT_REQUIRED

        self._create_socket()

    def _accept_applications(self): #override
        class PortClosed(Exception):
            pass

        while True:
            # blocks until there is a connection
            # keeps blocking when socket is closed
            try:
                if subprocess.call('which netstat > /dev/null 2>&1', shell=True) == 0:
                    if subprocess.call("netstat -pantu | grep -q %d" % self._port, shell=True) == 1:
                        raise PortClosed('Port is not found.')

                if self._context is None:
                    # python 2.6 - we either have to save the host key to a plain-readable file or do this
                    conn, addr = socket.socket.accept(self._sock)

                    keyfile = tempfile.NamedTemporaryFile(dir = '/tmp')
                    keyfile.write(self._keyfile_content)
                    keyfile.flush()

                    try:
                        conn = ssl.SSLSocket(conn,
                                             keyfile = keyfile.name,
                                             certfile = self._sock.certfile,
                                             server_side = True,
                                             cert_reqs = self._sock.cert_reqs,
                                             ssl_version = self._sock.ssl_version,
                                             ca_certs = self._sock.ca_certs,
                                             do_handshake_on_connect = self._sock.do_handshake_on_connect,
                                             suppress_ragged_eofs = self._sock.suppress_ragged_eofs)

                    except Exception:
                        conn.close()
                        raise

                    finally:
                        keyfile.close()

                else:
                    # python 2.7 - host key is saved in memory (in SSLContext)
                    conn, addr = self._sock.accept()

            except Exception as ex:
                if self._stop_flag.is_set():
                    return

                try:
                    if ex.errno == 9: # Bad file descriptor -> socket is closed
                        self.stop()
                        break
                except:
                    pass

                LOG.error('Application server connection failed with error: %s.' % str(sys.exc_info()[1]))

                if type(ex) is PortClosed:
                    # Create new socket if old one died
                    LOG.warning('Trying to create new socket.')

                    self._create_socket()

                continue

            thread.start_new_thread(self._process_application, (conn, addr))

    def _stop_accepting(self): #override
        """Shut down the socket."""
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except:
            pass

    def _process_application(self, conn, addr):
        """
        Communicate with the client and determine server actions.
        Communication is always conversational, starting with the client. This means recvmsg()
        can assume only one message will be sent in a single string (could still be split into
        multiple transmissions). We use a rudimentary protocol of preceding the message with
        the message length in decimal integer and a space (see SocketIO implementation).
        """

        io = SocketIO(conn, addr)
        master = self.dynamo_server.manager.master

        try:
            # check user authorization
            user_cert_data = conn.getpeercert()

            LOG.debug('New application request from %s:%s by %s' % (addr[0], addr[1], user_cert_data['subject']))

            dn = ''
            for rdn in user_cert_data['subject']:
                dn += '/' + '+'.join('%s=%s' % (DN_TRANSLATION[key], value) for key, value in rdn)

            user_info = master.identify_user(dn = dn, check_trunc = True)

            if user_info is None:
                io.send('failed', 'Unidentified user DN %s' % dn)
                return

            user_name, user_id = user_info[:2]

            io.send('OK', 'Connected')

            app_data = io.recv()

            command = app_data.pop('command')

            LOG.info('Accepted %s from %s:%s by %s' % (command, addr[0], addr[1], user_name))

            def act_and_respond(resp):
                success, msg = resp
                if success:
                    io.send('OK', msg)
                else:
                    io.send('failed', msg)

                return resp

            if command == 'poll':
                act_and_respond(self._poll_app(app_data['appid']))
                return

            elif not master.check_user_auth(user_name, 'admin', 'application') and not master.check_user_auth(user_name, 'operator', 'application'):
                io.send('failed', 'User not authorized')
                return

            if command == 'kill':
                act_and_respond(self._kill_app(app_data['appid']))

            elif command == 'add':
                act_and_respond(self._add_sequences(app_data['schedule'], user_name))

            elif command == 'remove':
                act_and_respond(self._delete_sequence(app_data['sequence'], user_name))

            elif command == 'start':
                if 'sequence' in app_data:
                    act_and_respond(self._start_sequence(app_data['sequence'], user_name))
                else:
                    act_and_respond(self._start_all_sequences())

            elif command == 'stop':
                if 'sequence' in app_data:
                    act_and_respond(self._stop_sequence(app_data['sequence'], user_name))
                else:
                    act_and_respond(self._stop_all_sequences())

            else:
                # new single application - get the work area path
                if 'path' in app_data:
                    # work area specified
                    workarea = app_data['path']
                else:
                    workarea = self._make_workarea()
                    if not workarea:
                        io.send('failed', 'Failed to create work area')

                if command == 'submit':
                    app_data['path'] = workarea
                    app_data['user_id'] = user_id
                    if io.host == 'localhost' or io.host == '127.0.0.1':
                        app_data['host'] = socket.gethostname()
                    else:
                        app_data['host'] = io.host

                    success, msg = act_and_respond(self._schedule_app(app_data))

                    if success and app_data['mode'] == 'synch':
                        # synchronous execution = client watches the app run
                        # client sends the socket address to connect stdout/err to
                        port_data = io.recv()
                        addr = (io.host, port_data['port'])

                        result = self._serve_synch_app(msg['appid'], msg['path'], addr)

                        io.send('OK', result)

                elif command == 'interact':
                    self._interact(workarea, io)

                    # cleanup
                    if 'path' not in app_data:
                        shutil.rmtree(workarea)

        except:
            exc_type, exc, tb = sys.exc_info()
            msg = '\n' + ''.join(traceback.format_tb(tb)) + '\n'
            msg += '%s: %s' % (exc_type.__name__, str(exc))
            io.send('failed', msg)
        finally:
            conn.close()

    def _interact(self, workarea, io):
        io.send('OK')
        port_data = io.recv()
        addr = (io.host, port_data['port'])

        args = (addr, workarea)

        proc = multiprocessing.Process(target = self._run_interactive_through_socket, name = 'interactive', args = args)
        proc.start()
        proc.join()

        LOG.info('Finished interactive session.')

    def _serve_synch_app(self, app_id, path, addr):
        conns = (socket.create_connection(addr), socket.create_connection(addr))

        stop_reading = threading.Event()

        for conn, name in zip(conns, ('stdout', 'stderr')):
            args = (path + '/_' + name, conn, stop_reading)
            th = threading.Thread(target = tail_follow, name = name, args = args)
            th.daemon = True
            th.start()

        msg = self.wait_synch_app_queue(app_id) # {'status': status, 'exit_code': exit_code}
        self.remove_synch_app_queue(app_id)

        # not an elegant solution but we need to keep the reader threads alive for just a bit longer
        time.sleep(1)

        stop_reading.set()

        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except:
                pass
            conn.close()

        return {'status': AppManager.status_name(msg['status']), 'exit_code': msg['exit_code']}

    def _run_interactive_through_socket(self, addr, workarea):
        conns = (socket.create_connection(addr), socket.create_connection(addr))
        stdout = conns[0].makefile('w')
        stderr = conns[1].makefile('w')

        if addr[0] == 'localhost' or addr[0] == '127.0.0.1':
            is_local = True
        else:
            is_local = (addr[0] == socket.gethostname())

        # use the receive side of conns[0] for stdin
        make_console = lambda l: SocketConsole(conns[0], l)

        self.dynamo_server.run_interactive(workarea, is_local, make_console, stdout, stderr)

        stdout.close()
        stderr.close()

        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except:
                pass
            conn.close()

    def _create_socket(self):
        LOG.info('Creating new socket.')

        if self._context is None:
            # python 2.6
            keyfile = tempfile.NamedTemporaryFile()
            keyfile.write(self._keyfile_content)
            keyfile.flush()

            try:
                self._sock = ssl.wrap_socket(socket.socket(socket.AF_INET), server_side = True,
                                             certfile = self._certfile, keyfile = keyfile.name,
                                             cert_reqs = ssl.CERT_REQUIRED, ca_certs = self._capath)
            finally:
                keyfile.close()

        else:
            self._sock = self._context.wrap_socket(socket.socket(socket.AF_INET), server_side = True)

        # allow reconnect to the same port even when it is in TIME_WAIT
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        LOG.info('Socket created successfully.')

        for _ in xrange(10):
            try:
                self._sock.bind(('', self._port))
                LOG.info('Socket bound successfully.')
                break
            except socket.error as err:
                if err.errno == 98: # address already in use
                    # check the server status before retrying - could be shut down
                    self.dynamo_server.check_status_and_connection()
                    # then print and retry
                    LOG.warning('Cannot bind to port %d. Retrying..', self._port)
                    time.sleep(5)
        else:
            # exhausted attempts
            LOG.error('Failed to bind to port %d.', self._port)
            raise
    
        self._sock.listen(5)


class SocketConsole(code.InteractiveConsole):
    """
    Console where input comes from a socket. Because the core of the console uses the python
    exec statement, we cannot just re-implement write() to send to a socket, and have to replace
    sys.stdout and sys.stderr with socket files.
    """

    def __init__(self, conn, locals = None, filename = '<dynamo>'):
        code.InteractiveConsole.__init__(self, locals, filename)

        self._conn = conn
        self._lines = []
        self._last_line = ''

        self._buffer = ''
        self._expected_length = ''

    def write(self, data):
        # InteractiveConsole.write() only writes to stderr and does not flush.
        # If stderr is actually a socket makefile(), no data will be sent unless flushed.

        sys.stderr.write(data)
        try:
            sys.stderr.flush()
        except:
            pass

    def raw_input(self, prompt = ''):
        sys.stdout.write(prompt)
        try:
            sys.stdout.flush()
        except:
            return ''

        data = ''

        while len(self._lines) == 0 or len(data) != 0:
            if len(data) == 0:
                # receive data chunk
                chunk = self._conn.recv(2048)
                if not chunk:
                    # socket closed
                    raise EOFError()

                data += chunk

            if len(self._buffer) == 0:
                # if we are at the beginning of the chunk
                pos = data.find(' ')
                if pos == -1:
                    # received chunk is not even the full word for the data length
                    self._expected_length += data
                    continue

                self._expected_length += data[:pos]
                data = data[pos + 1:]

            expected_length = int(self._expected_length)

            if expected_length == 0:
                self._expected_length = ''
                raise EOFError()

            # read the data into buffer
            read_length = expected_length - len(self._buffer)
            self._buffer += data[:read_length]

            # shift data
            data = data[read_length:]

            if len(self._buffer) < expected_length:
                # data didn't contain the full content
                continue

            # now we have the buffer with intended length
            # note that we don't guarantee the buffer ends nicely with a newline
            # i.e. the buffer may say it's 30 characters long and send 30 characters,
            # but may not be the whole command line

            # split buffer into lines
            while True:
                newline = self._buffer.find('\n')
                if newline == -1:
                    self._last_line += self._buffer
                    break
                else:
                    self._last_line += self._buffer[:newline]
                    self._lines.append(self._last_line)
                    self._last_line = ''
                    self._buffer = self._buffer[newline + 1:]

            self._expected_length = ''

        return self._lines.pop(0)

