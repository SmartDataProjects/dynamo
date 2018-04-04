class WebServer(object):
    def __init__(self, config, dynamoserver):
        self._dynamo = dynamoserver

    def main(self, environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return map(str, environ.items())
