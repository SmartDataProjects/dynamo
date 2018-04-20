import json

from dynamo.web.get import getters
from dynamo.web.set import setters
from dynamo.utils.interface.mysql import MySQL

class WebServer(object):
    def __init__(self, config, dynamoserver):
        self._dynamo = dynamoserver
        self._registry = MySQL(config.registry) # will replace with something more abstract

    def main(self, environ, start_response):
        name = environ['SCRIPT_NAME'].strip('/')
        command = environ['PATH_INFO'][1:]
        request = {}
        for query in environ['QUERY_STRING'].split('&'):
            key, _, value = query.partition('=')
            request[key] = value
        
        if name == 'get':
            content = getters[command](request, self._dynamo.inventory, self._registry, self._dynamo.manager.master)
        elif name == 'set':
            content = setters[command](request, self._dynamo.inventory, self._registry, self._dynamo.manager.master)

        start_response('200 OK', [('Content-Type', 'text/plain')])
        return json.dumps(content)
