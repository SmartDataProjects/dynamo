import os
import sys
import time
import traceback
import json
import logging
import logging.handlers
import socket
import collections
import warnings
import multiprocessing
import cStringIO
from cgi import parse_qs, escape
from flup.server.fcgi_fork import WSGIServer

import dynamo.core.serverutils as serverutils
import dynamo.web.exceptions as exceptions
# Actual modules imported at the bottom of this file
from dynamo.web.modules import modules, load_modules
from dynamo.web.modules._html import HTMLMixin

from dynamo.utils.transform import unicode2str
from dynamo.utils.log import reset_logger

LOG = logging.getLogger(__name__)

class WebServer(object):
    User = collections.namedtuple('User', ['name', 'dn', 'id', 'authlist'])

    @staticmethod
    def format_dn(dn_string):
        """Read the DN string in the environ and return (user name, user id)."""
        dn_parts = []
        start = 0
        end = 0
        while True:
            end = dn_string.find(',', end)
            if end == -1:
                dn_parts.append(dn_string[start:])
                break

            if end == 0:
                raise exceptions.AuthorizationError()

            if dn_string[end - 1] == '\\':
                # if this was an escaped comma, move ahead
                end += 1
                continue

            dn_parts.append(dn_string[start:end])
            end += 2 # skip ', '
            start = end

        dn = ''
        for part in dn_parts:
            key, _, value = part.partition(' = ')
            dn += '/' + key + '=' + value

        return dn


    def __init__(self, config, dynamo_server):
        self.socket = config.socket
        self.modules_config = config.modules_config.clone()
        self.dynamo_server = dynamo_server

        # Preforked WSGI server
        # Preforking = have at minimum min_idle and at maximum max_idle child processes listening to the out-facing port.
        # There can be at most max_procs children. Each child process is single-use to ensure changes to shared resources (e.g. inventory)
        # made in a child process does not affect the other processes.
        prefork_config = {'minSpare': config.get('min_idle', 1), 'maxSpare': config.get('max_idle', 5), 'maxChildren': config.get('max_procs', 10), 'maxRequests': 1}
        self.wsgi_server = WSGIServer(self.main, bindAddress = config.socket, umask = 0, **prefork_config)

        self.server_proc = None

        self.active_count = multiprocessing.Value('I', 0, lock = True)

        HTMLMixin.contents_path = config.contents_path
        # common mixin class used by all page-generating modules
        with open(HTMLMixin.contents_path + '/html/header_common.html') as source:
            HTMLMixin.header_html = source.read()
        with open(HTMLMixin.contents_path + '/html/footer_common.html') as source:
            HTMLMixin.footer_html = source.read()

        # cookie string -> (user name, user id)
        self.known_users = {}

        # Log file path (start a rotating log if specified)
        self.log_path = None

        self.debug = config.get('debug', False)

    def start(self):
        if self.server_proc and self.server_proc.is_alive():
            raise RuntimeError('Web server is already running')

        self.server_proc = multiprocessing.Process(target = self._serve)
        self.server_proc.daemon = True
        self.server_proc.start()

        LOG.info('Started web server (PID %d).', self.server_proc.pid)

    def stop(self):
        LOG.info('Stopping web server (PID %d).', self.server_proc.pid)

        self.server_proc.terminate()
        LOG.debug('Waiting for web server to join.')
        self.server_proc.join(5)

        if self.server_proc.is_alive():
            # SIGTERM got ignored
            LOG.info('Web server failed to stop. Sending KILL signal..')
            try:
                os.kill(self.server_proc.pid, signal.SIGKILL)
            except:
                pass

            self.server_proc.join(5)

            if self.server_proc.is_alive():
                LOG.warning('Web server (PID %d) is stuck.', self.server_proc.pid)
                self.server_proc = None
                return

        LOG.debug('Web server joined.')

        self.server_proc = None

    def restart(self):
        LOG.info('Restarting web server (PID %d).', self.server_proc.pid)

        # Replace the active_count by a temporary object (won't be visible from subprocs)
        old_active_count = self.active_count
        self.active_count = multiprocessing.Value('I', 0, lock = True)

        # A new WSGI server will overtake the socket. New requests will be handled by new_server_proc
        LOG.debug('Starting new web server.')
        new_server_proc = multiprocessing.Process(target = self._serve)
        new_server_proc.daemon = True
        new_server_proc.start()

        # Drain and stop the main server
        LOG.debug('Waiting for web server to drain.')
        elapsed = 0.
        while old_active_count.value != 0:
            time.sleep(0.2)
            elapsed += 0.2
            if elapsed >= 10.:
                break

        self.stop()

        self.server_proc = new_server_proc

        LOG.info('Started web server (PID %d).', self.server_proc.pid)

    def _serve(self):
        if self.log_path:
            reset_logger()

            root_logger = logging.getLogger()
            log_handler = logging.handlers.RotatingFileHandler(self.log_path, maxBytes = 10000000, backupCount = 100)
            log_handler.setFormatter(logging.Formatter(fmt = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
            root_logger.addHandler(log_handler)

        # If the inventory loads super-fast in the main server, we get reset while loading the defaults
        # Block the terminate signal to avoid KeyboardInterrupt error messages
        try:
            # Ignore one specific warning issued by accident when a web page crashes and dumps a stack trace
            # cgitb scans all exception attributes with dir(exc) + getattr(exc, attr) which results in accessing
            # exception.message, a deprecated attribute.
            warnings.filterwarnings('ignore', 'BaseException.message.*', DeprecationWarning, '.*cgitb.*', 173)

            load_modules()
    
            # Set up module defaults
            # Using the same piece of code as serverutils, but only picking up fullauth or all configurations
            for key, config in self.dynamo_server.defaults_config.items():
                try:
                    myconf = config['fullauth']
                except KeyError:
                    try:
                        myconf = config['all']
                    except KeyError:
                        continue
        
                modname, clsname = key.split(':')
                module = __import__('dynamo.' + modname, globals(), locals(), [clsname])
                cls = getattr(module, clsname)
        
                cls.set_default(myconf)
    
        except KeyboardInterrupt:
            os._exit(0)

        try:
            self.wsgi_server.run()
        except SystemExit as exc:
            LOG.debug('Web server subprocess %d exiting', os.getpid())
            # Server subprocesses terminate with sys.exit and land here
            # Because sys.exit performs garbage collection, which can disrupt shared resources (e.g. close connection to DB)
            # we need to translate it to os._exit
            os._exit(exc.code)

    def main(self, environ, start_response):
        # Increment the active count so that the parent process won't be killed before this function returns
        with self.active_count.get_lock():
            self.active_count.value += 1

            try:
                agent = environ['HTTP_USER_AGENT']
            except KeyError:
                agent = 'Unknown'

            # Log file is a shared resource - write within the lock
            LOG.info('%s-%s %s (%s:%s %s)', environ['REQUEST_SCHEME'], environ['REQUEST_METHOD'], environ['REQUEST_URI'], environ['REMOTE_ADDR'], environ['REMOTE_PORT'], agent)

        # Then immediately switch to logging to a buffer
        root_logger = logging.getLogger()
        stream = cStringIO.StringIO()
        original_handler = root_logger.handlers.pop()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter(fmt = '%(levelname)s:%(name)s: %(message)s'))
        root_logger.addHandler(handler)

        stdout = sys.stdout
        stderr = sys.stderr

        sys.stdout = stream
        sys.stderr = stream

        try:
            self.code = 200 # HTTP response code
            self.content_type = 'application/json' # content type string
            self.headers = [] # list of header tuples
            self.callback = None # set to callback function name if this is a JSONP request
            self.message = '' # string
            self.phedex_request = '' # backward compatibility

            content = self._main(environ)

            # Maybe we can use some standard library?
            if self.code == 200:
                status = 'OK'
            elif self.code == 400:
                status = 'Bad Request'
            elif self.code == 403:
                status = 'Forbidden'
            elif self.code == 404:
                status = 'Not Found'
            elif self.code == 500:
                status = 'Internal Server Error'
            elif self.code == 503:
                status = 'Service Unavailable'

            if self.content_type == 'application/json':
                if self.phedex_request != '':
                    if type(content) is not dict:
                        self.code == 500
                        status = 'Internal Server Error'
                    else:
                        url = '%s://%s' % (environ['REQUEST_SCHEME'], environ['HTTP_HOST'])
                        if (environ['REQUEST_SCHEME'] == 'http' and environ['SERVER_PORT'] != '80') or \
                           (environ['REQUEST_SCHEME'] == 'https' and environ['SERVER_PORT'] != '443'):
                            url += '%s' % environ['SERVER_PORT']
                        url += environ['REQUEST_URI']
    
                        json_data = {'phedex': {'call_time': 0, 'instance': 'prod', 'request_call': self.phedex_request,
                                                'request_date': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()), 'request_timestamp': time.time(),
                                                'request_url': url, 'request_version': '2.2.1'}}
                        json_data['phedex'].update(content)
    
                        content = json.dumps(json_data)

                else:
                    json_data = {'result': status, 'message': self.message}
                    if content is not None:
                        json_data['data'] = content
    
                    # replace content with the json string
                    start = time.time()
                    if self.callback is not None:
                        LOG.info("Callback is not None")
                        LOG.info(json_data)
                        content = '%s(%s)' % (self.callback, json.dumps(json_data))
                    else:
                        LOG.info("Callback is None")
                        LOG.info(json_data)
                        content = json.dumps(json_data)

                    root_logger.info('Make JSON: %s seconds', time.time() - start)

            headers = [('Content-Type', self.content_type)] + self.headers

            start_response('%d %s' % (self.code, status), headers)

            return content + '\n'

        finally:
            sys.stdout = stdout
            sys.stderr = stderr

            root_logger.handlers.pop()
            root_logger.addHandler(original_handler)

            delim = '--------------'
            log_tmp = stream.getvalue().strip()
            if len(log_tmp) == 0:
                log = 'empty log'
            else:
                log = 'return:\n%s\n%s%s' % (delim, ''.join('  %s\n' % line for line in log_tmp.split('\n')), delim)

            with self.active_count.get_lock():
                LOG.info('%s-%s %s (%s:%s) %s', environ['REQUEST_SCHEME'], environ['REQUEST_METHOD'], environ['REQUEST_URI'], environ['REMOTE_ADDR'], environ['REMOTE_PORT'], log)
                self.active_count.value -= 1

    def _main(self, environ):
        """
        Body of the WSGI callable. Steps:
        1. Determine protocol. If HTTPS, identify the user.
        2. If js or css is requested, respond.
        3. Find the module class and instantiate it.
        4. Parse the query string into a dictionary.
        5. Call the run() function of the module class.
        6. Respond.
        """

        authorizer = None

        ## Step 1
        if environ['REQUEST_SCHEME'] == 'http':
            # No auth
            user, dn, user_id = None, None, 0
            authlist = []

        elif environ['REQUEST_SCHEME'] == 'https':
            authorizer = self.dynamo_server.manager.master.create_authorizer()

            # Client DN must match a known user
            try:
                dn = WebServer.format_dn(environ['SSL_CLIENT_S_DN'])
                userinfo = authorizer.identify_user(dn = dn, check_trunc = True)
                if userinfo is None:
                    raise exceptions.AuthorizationError()

                user, user_id, dn = userinfo

            except exceptions.AuthorizationError:
                self.code = 403
                self.message = 'Unknown user. Client name: %s' % environ['SSL_CLIENT_S_DN']
                return 
            except:
                return self._internal_server_error()

            authlist = authorizer.list_user_auth(user)

        else:
            self.code = 400
            self.message = 'Only HTTP or HTTPS requests are allowed.'
            return

        ## Step 2
        mode = environ['SCRIPT_NAME'].strip('/')

        if mode == 'js' or mode == 'css':
            try:
                source = open(HTMLMixin.contents_path + '/' + mode + environ['PATH_INFO'])
            except IOError:
                self.code = 404
                self.content_type = 'text/plain'
                return 'Invalid request %s%s.\n' % (mode, environ['PATH_INFO'])
            else:
                if mode == 'js':
                    self.content_type = 'text/javascript'
                else:
                    self.content_type = 'text/css'

                content = source.read() + '\n'
                source.close()

                return content

        ## Step 3
        if mode != 'data' and mode != 'web' and mode != 'registry' and mode != 'phedexdata': # registry and phedexdata for backward compatibility
            self.code = 404
            self.message = 'Invalid request %s.' % mode
            return

        if mode == 'phedexdata':
            mode = 'data'
            self.phedex_request = environ['PATH_INFO'][1:]

        module, _, command = environ['PATH_INFO'][1:].partition('/')

        try:
            cls = modules[mode][module][command]
        except KeyError:
            # Was a new module added perhaps?
            load_modules()
            try: # again
                cls = modules[mode][module][command]
            except KeyError:
                self.code = 404
                self.message = 'Invalid request %s/%s.' % (module, command)
                return

        try:
            provider = cls(self.modules_config)
        except:
            return self._internal_server_error()

        if provider.must_authenticate and user is None:
            self.code = 400
            self.message = 'Resource only available with HTTPS.'
            return

        if provider.write_enabled:
            self.dynamo_server.manager.master.lock()

            try:
                if self.dynamo_server.manager.master.inhibit_write():
                    # We need to give up here instead of waiting, because the web server processes will be flushed out as soon as
                    # inventory is updated after the current writing process is done
                    self.code = 503
                    self.message = 'Server cannot execute %s/%s at the moment because the inventory is being updated.' % (module, command)
                    return
                else:
                    self.dynamo_server.manager.master.start_write_web(socket.gethostname(), os.getpid())
                    # stop is called from the DynamoServer upon successful inventory update

            except:
                self.dynamo_server.manager.master.stop_write_web()
                raise

            finally:
                self.dynamo_server.manager.master.unlock()

        if provider.require_authorizer:
            if authorizer is None:
                authorizer = self.dynamo_server.manager.master.create_authorizer()

            provider.authorizer = authorizer

        if provider.require_appmanager:
            provider.appmanager = self.dynamo_server.manager.master.create_appmanager()

        try:
            ## Step 4
            post_request = None

            if environ['REQUEST_METHOD'] == 'POST':
                try:
                    content_type = environ['CONTENT_TYPE']
                except KeyError:
                    content_type = 'application/x-www-form-urlencoded'

                # In principle we should grab CONTENT_LENGTH from environ and only read as many bytes as given, but wsgi.input seems to know where the EOF is
                try:
                    content_length = environ['CONTENT_LENGTH']
                except KeyError:
                    # length -1: rely on wsgi.input having an EOF at the end
                    content_length = -1

                post_data = environ['wsgi.input'].read(content_length)

                # Even though our default content type is URL form, we check if this is a JSON
                try:
                    LOG.info("Printing post_data")
                    LOG.info(post_data)
                    json_data = json.loads(post_data)
                except:
                    if content_type == 'application/json':
                        self.code = 400
                        self.message = 'Could not parse input.'
                        return
                else:
                    content_type = 'application/json'
                    provider.input_data = json_data
                    unicode2str(provider.input_data)

                if content_type == 'application/x-www-form-urlencoded':
                    try:
                        LOG.info("Printing post_data 2")
                        LOG.info(post_data)
                        post_request = parse_qs(post_data)
                    except:
                        self.code = 400
                        self.message = 'Could not parse input.'
                elif content_type != 'application/json':
                    self.code = 400
                    self.message = 'Unknown Content-Type %s.' % content_type

            get_request = parse_qs(environ['QUERY_STRING'])

            if post_request is not None:
                for key, value in post_request.iteritems():
                    if key in get_request:
                        # return dict of parse_qs is {key: list}
                        get_request[key].extend(post_request[key])
                    else:
                        get_request[key] = post_request[key]

            unicode2str(get_request)

            request = {}
            for key, value in get_request.iteritems():
                if key.endswith('[]'):
                    key = key[:-2]
                    request[key] = map(escape, value)
                else:
                    if len(value) == 1:
                        request[key] = escape(value[0])
                    else:
                        request[key] = map(escape, value)

            ## Step 5
            caller = WebServer.User(user, dn, user_id, authlist)

            if self.dynamo_server.inventory.loaded:
                inventory = self.dynamo_server.inventory.create_proxy()
                if provider.write_enabled:
                    inventory._update_commands = []
            else:
                inventory = DummyInventory()

            content = provider.run(caller, request, inventory)

            if provider.write_enabled:
                self.dynamo_server._send_updates(inventory)
            
        except (exceptions.AuthorizationError, exceptions.ResponseDenied, exceptions.MissingParameter,
                exceptions.ExtraParameter, exceptions.IllFormedRequest, exceptions.InvalidRequest) as ex:
            self.code = 400
            self.message = str(ex)
            return
        except exceptions.TryAgain as ex:
            self.code = 503
            self.message = str(ex)
            return
        except:
            return self._internal_server_error()

        ## Step 6
        self.message = provider.message
        self.content_type = provider.content_type
        self.headers = provider.additional_headers
        if 'callback' in request:
            self.callback = request['callback']

        return content

    def _internal_server_error(self):
        self.code = 500
        self.content_type = 'text/plain'

        exc_type, exc, tb = sys.exc_info()
        if self.debug:
            response = 'Caught exception %s while waiting for task to complete.\n' % exc_type.__name__
            response += 'Traceback (most recent call last):\n'
            response += ''.join(traceback.format_tb(tb)) + '\n'
            response += '%s: %s\n' % (exc_type.__name__, str(exc))
        else:
            response = 'Internal server error! (' + exc_type.__name__ + ': ' + str(exc) + ')\n'

        LOG.error(response)

        return response

class DummyInventory(object):
    """
    Inventory placeholder that just throws a 503. To be used when inventory is not loaded yet.
    We start the web server even before the inventory is loaded because not all web modules use inventory.
    """
    def __getattr__(self, attr):
        raise exceptions.TryAgain('Dynamo server is starting. Please try again in a few moments.')
