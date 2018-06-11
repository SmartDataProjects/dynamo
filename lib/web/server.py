import os
import sys
import time
import traceback
import json
import logging
import socket
import collections
import multiprocessing
from cgi import FieldStorage
from flup.server.fcgi_fork import WSGIServer

import dynamo.core.serverutils as serverutils
import dynamo.web.exceptions as exceptions
# Actual modules imported at the bottom of this file
from dynamo.web.modules import modules
from dynamo.web.modules._html import HTMLMixin

from dynamo.utils.transform import unicode2str

LOG = logging.getLogger(__name__)

class WebServer(object):
    User = collections.namedtuple('User', ['name', 'id', 'authlist'])

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

        HTMLMixin.contents_path = config.contents_path
        # common mixin class used by all page-generating modules
        with open(HTMLMixin.contents_path + '/html/header_common.html') as source:
            HTMLMixin.header_html = source.read()
        with open(HTMLMixin.contents_path + '/html/footer_common.html') as source:
            HTMLMixin.footer_html = source.read()

        # cookie string -> (user name, user id)
        self.known_users = {}

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
        self.server_proc.join()
        LOG.debug('Web server joined.')

        self.server_proc = None

    def _serve(self):
        try:
            self.wsgi_server.run()
        except SystemExit as exc:
            LOG.debug('Web server subprocess %d exiting', os.getpid())
            # Server subprocesses terminate with sys.exit and land here
            # Because sys.exit performs garbage collection, which can disrupt shared resources (e.g. close connection to DB)
            # we need to translate it to os._exit
            os._exit(exc.code)

    def main(self, environ, start_response):
        """
        WSGI callable. Steps:
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
            user, user_id = None, 0
            authlist = []

        elif environ['REQUEST_SCHEME'] == 'https':
            authorizer = self.dynamo_server.manager.master.create_authorizer()

            # Client DN must match a known user
            try:
                user, user_id = self.identify_user(environ, authorizer)
            except exceptions.AuthorizationError:
                start_response('403 Forbidden', [('Content-Type', 'text/plain')])
                return 'Unknown user.\nClient name: %s\n' % environ['SSL_CLIENT_S_DN']
            except:
                return self._internal_server_error(start_response)

            authlist = authorizer.list_user_auth(user)

        else:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return 'Only HTTP or HTTPS requests are allowed.\n'

        ## Step 2
        mode = environ['SCRIPT_NAME'].strip('/')

        if mode == 'js' or mode == 'css':
            try:
                source = open(HTMLMixin.contents_path + '/' + mode + environ['PATH_INFO'])
            except IOError:
                start_response('404 Not Found', [('Content-Type', 'text/plain')])
                return 'Invalid request %s%s.\n' % (mode, environ['PATH_INFO'])
            else:
                content = source.read()
                source.close()
                if mode == 'js':
                    ctype = 'text/javascript'
                else:
                    ctype = 'text/css'

                start_response('200 OK', [('Content-Type', ctype)])
                return content + '\n'

        ## Step 3
        if mode != 'data' and mode != 'web':
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return 'Invalid request %s.\n' % mode

        module, _, command = environ['PATH_INFO'][1:].partition('/')

        try:
            cls = modules[mode][module][command]
        except KeyError:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return 'Invalid request %s/%s.\n' % (module, command)

        try:
            provider = cls(self.modules_config)
        except:
            return self._internal_server_error(start_response)

        if provider.write_enabled:
            self.dynamo_server.manager.master.lock()

            try:
                if self.dynamo_server.manager.master.inhibit_write():
                    # We need to give up here instead of waiting, because the web server processes will be flushed out as soon as
                    # inventory is updated after the current writing process is done
                    start_response('503 Service Unavailable', [('Content-Type', 'text/plain')])
                    return 'Server cannot execute %s/%s at the moment because the inventory is being updated.\n' % (module, command)
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

        try:
            ## Step 4
            if 'CONTENT_TYPE' in environ and environ['CONTENT_TYPE'] == 'application/json':
                try:
                    request = json.loads(environ['wsgi.input'].read())
                except:
                    start_response('400 Bad Request', [('Content-Type', 'text/plain')])
                    return 'Could not parse input.\n'
            else:
                # Use FieldStorage to parse URL-encoded GET and POST requests
                fstorage = FieldStorage(fp = environ['wsgi.input'], environ = environ, keep_blank_values = True)
                if fstorage.list is None:
                    start_response('400 Bad Request', [('Content-Type', 'text/plain')])
                    return 'Could not parse input.\n'

                request = {}
                for item in fstorage.list:
                    if item.name.endswith('[]'):
                        key = item.name[:-2]
                        try:
                            request[key].append(item.value)
                        except KeyError:
                            request[key] = [item.value]
                    else:
                        request[item.name] = item.value

            unicode2str(request)
    
            ## Step 5
            caller = WebServer.User(user, user_id, authlist)

            inventory = self.dynamo_server.inventory.create_proxy()
            if provider.write_enabled:
                inventory._update_commands = []

            content = provider.run(caller, request, inventory)

            if provider.write_enabled:
                # TODO make web server log to a separate file
                serverutils.send_updates(inventory, self.dynamo_server.inventory_update_queue, silent = True)
            
        except exceptions.AuthorizationError:
            start_response('403 Forbidden', [('Content-Type', 'text/plain')])
            return 'User not authorized to perform the request.\n'
        except exceptions.MissingParameter as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            msg = 'Missing required parameter "%s"' % ex.param_name
            if ex.context is not None:
                msg += ' in %s' % ex.context
            msg += '.\n'
            return msg
        except exceptions.ExtraParameter as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            msg = 'Parameter "%s" not expected' % ex.param_name
            if ex.context is not None:
                msg += ' in %s' % ex.context
            msg += '.\n'
            return msg
        except exceptions.IllFormedRequest as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            msg = 'Parameter "%s" has illegal value "%s".' % (ex.param_name, ex.value)
            if ex.hint is not None:
                msg += ' ' + ex.hint + '.'
            if ex.allowed is not None:
                msg += ' Allowed values: [%s]' % ['"%s"' % v for v in ex.allowed]
            return msg + '\n'
        except exceptions.ResponseDenied as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return 'Server denied response due to: %s\n' % str(ex)
        except:
            return self._internal_server_error(start_response)

        ## Step 6
        headers = [('Content-Type', provider.content_type)] + provider.additional_headers
        start_response('200 OK', headers)

        if mode == 'data' and provider.content_type == 'application/json':
            data_str = json.dumps(content)

            if 'callback' in request:
                # JSONP request
                return request['callback'] + '(' + data_str + ')'
            else:
                # Normal JSON
                return data_str + '\n'
        else:
            return content

    def identify_user(self, environ, authorizer):
        """Read the DN string in the environ and return (user name, user id)."""
        
        dn_string = environ['SSL_CLIENT_S_DN']
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

        userinfo = authorizer.identify_user(dn = dn, check_trunc = True, with_id = True)

        if userinfo is None:
            raise exceptions.AuthorizationError()

        return userinfo

    def _internal_server_error(self, start_fnc):
        start_fnc('500 Internal Server Error', [('Content-Type', 'text/plain')])
        exc_type, exc, tb = sys.exc_info()
        if self.debug:
            response = 'Caught exception %s while waiting for task to complete.\n' % exc_type.__name__
            response += 'Traceback (most recent call last):\n'
            response += ''.join(traceback.format_tb(tb)) + '\n'
            response += '%s: %s\n' % (exc_type.__name__, str(exc))
            return response
        else:
            return 'Internal server error! (' + exc_type.__name__ + ': ' + str(exc) + ')\n'
