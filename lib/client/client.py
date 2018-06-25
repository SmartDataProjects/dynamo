import sys
import json
import time
import urllib2

from dynamo.utils.interface.webservice import RESTService, GET, POST
from dynamo.dataformat import Configuration

class DynamoWebClient(RESTService):
    def __init__(self, config):
        # Attempt indefinitely
        config.num_attempts = 1

        if config.need_auth:
            config.url_base = config.url_base.replace('http://', 'https://')
        else:
            config.url_base = config.url_base.replace('https://', 'http://')
            config.auth_handler = 'None'

        RESTService.__init__(self, config)

    def make_request(self, resource = '', options = [], method = GET, format = 'url', retry_on_error = True, timeout = 0):
        request = self._form_request(resource, options = options, method = method, format = format)

        while True:
            try:
                response = self._request_one(request, timeout)
                break
            except urllib2.HTTPError as err:
                if err.code == 503:
                    sys.stderr.write('Server is unavailable: %s\n' % response.text)
                    time.sleep(2)

                else:
                    print err.read()
                    response = json.loads(err.read())
                    break

        if response['result'] != 'OK':
            sys.stderr.write('Failed to execute request. Server responded: %s' % response['message'])
            return None

        return response['data']
