import json

from dynamo.web.get import getters
from dynamo.web.set import setters

class WebServer(object):
    def __init__(self, config, dynamoserver):
        self._dynamo = dynamoserver

    def main(self, environ, start_response):
        name = environ['SCRIPT_NAME'].strip('/')
        command = tuple(environ['PATH_INFO'][1:].split('/'))
        request = {}
        for query in environ['QUERY_STRING'].split('&'):
            key, _, value = query.partition('=')
            request[key] = value
        
        if name == 'get':
            content = getters[command](self._dynamo.inventory, self._dynamo.manager.master, request)
        elif name == 'set':
            content = setters[command](self._dynamo.inventory, self._dynamo.manager.master, request)

        start_response('200 OK', [('Content-Type', 'text/plain')])
        return json.dumps(content)
