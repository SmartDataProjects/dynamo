import os
import sys
import re
import json
import collections
from cgi import FieldStorage
from flup.server.fcgi import WSGIServer

import dynamo.web.exceptions as exceptions
# Actual modules imported at the bottom of this file
from dynamo.web.modules import modules

class WebServer(object):
    User = collections.namedtuple('User', ['name', 'id', 'authlist'])

    def __init__(self, config, inventory, authorizer):
        self.socket = config.socket
        self.modules_config = config.modules_config.clone()
        self.inventory = inventory
        self.authorizer = authorizer

        # cookie string -> (user name, user id)
        self.known_users = {}

    def start(self):
        # Thread-based WSGI server
        WSGIServer(self.main, bindAddress = self.socket, umask = 0).run()

    def main(self, environ, start_response):
        """
        WSGI callable. Steps:
        1. Identify the user if HTTPS
        2. Find the module class.
        3. Parse the query string into a dictionary.
        4. Call the run() function of the module class.
        5. Respond.
        """

        ## Step 1
        if environ['REQUEST_SCHEME'] == 'http':
            # No auth
            user, user_id = None, 0
            authlist = []

        elif environ['REQUEST_SCHEME'] == 'https':
            # Client DN must match a known user
            try:
                user, user_id = self.identify_user(environ)
            except:
                start_response('403 Forbidden', [('Content-Type', 'text/plain')])
                return 'Unknown user.\nClient name: %s' % environ['SSL_CLIENT_S_DN']

            authlist = self.authorizer.list_user_auth(user)

        else:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return 'Only HTTP or HTTPS requests are allowed.'

        caller = WebServer.User(user, user_id, authlist)

        ## Step 2
        mode = environ['SCRIPT_NAME'].strip('/')
        if mode != 'data' and mode != 'web':
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return 'Invalid request %s.' % mode

        module, _, command = environ['PATH_INFO'][1:].partition('/')

        try:
            cls = modules[mode][module][command]
        except KeyError:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return 'Invalid request %s/%s.' % (module, command)

        ## Step 3
        # FieldStorage is a dict-like class that holds both GET and POST requests
        request = FieldStorage(fp = environ['wsgi.input'], environ = environ, keep_blank_values = True)

        ## Step 4
        try:
            content = cls(self.modules_config).run(caller, request, self.inventory)
        except exceptions.AuthorizationError:
            start_response('403 Forbidden', [('Content-Type', 'text/plain')])
            return 'User not authorized to perform the request.'
        except exceptions.MissingParameter as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return 'Missing required parameter "%s".' % ex.param_name
        except exceptions.IllFormedRequest as ex:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return 'Parameter "%s" has illegal value "%s".' % (ex.param_name, ex.value)
        except:
            start_response('500 Internal Server Error', [('Content-Type', 'text/plain')])
            return 'Exception: ' + str(sys.exc_info()[1])

        ## Step 5
        if mode == 'data':
            start_response('200 OK', [('Content-Type', 'application/json')])
            return json.dumps(content)
        else:
            start_response('200 OK', [('Content-Type', 'text/html')])
            return content

    def identify_user(self, environ):
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
                raise RuntimeError()

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

        userinfo = self.authorizer.identify_user(dn = dn, with_id = True)

        if userinfo is None:
            raise RuntimeError()

        return userinfo
