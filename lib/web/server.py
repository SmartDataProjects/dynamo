import sys
import json

import dynamo.web.exceptions as exceptions

class WebServer(object):
    class DynamoHandle(object):
        # One-stop object binding all Dynamo objects
        def __init__(self, server, config):
            self.inventory = server.inventory
            self.server_manager = server.manager
            self.registry = server.registry
            #self.history = 

    def __init__(self, config, dynamoserver):
        self.handle = WebServer.DynamoHandle(dynamoserver, config)

    def main(self, environ, start_response):
        name = environ['SCRIPT_NAME'].strip('/')
        command = environ['PATH_INFO'][1:]

        try:
            if name == 'get':
                func = getters[command]
            elif name == 'set':
                func = setters[command]
            else:
                raise KeyError(name)
        except KeyError:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return 'Invalid request %s/%s.' % (name, command)

        request = {}
        for query in environ['QUERY_STRING'].split('&'):
            key, _, value = query.partition('=')
            request[key] = value

        try:
            try:
                content = func(request, self.handle)
            except exceptions.MissingParameter as ex:
                msg = 'Missing required parameter "%s".' % ex.param_name
                raise
            except exceptions.IllFormedRequest as ex:
                msg = 'Parameter "%s" has illegal value "%s".' % (ex.param_name, ex.value)
                raise
            except:
                start_response('500 Internal Server Error', [('Content-Type', 'text/plain')])
                return 'Exception: ' + str(sys.exc_info()[1])

        except:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return msg

        start_response('200 OK', [('Content-Type', 'application/json')])
        return json.dumps(content)
