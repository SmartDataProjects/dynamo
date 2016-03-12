import urllib
import urllib2
import httplib
import json
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = config.webservice.x509_key
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.create_connection, req)

    def create_connection(self, host, timeout = 300):
        return httplib.HTTPSConnection(host, key_file = self.key, cert_file = self.cert)


class RESTService(object):
    """
    An interface to RESTful APIs (e.g. PhEDEx, DBS) with X509 authentication.
    make_request will take the REST "command" and a list of options as arguments.
    Options are chained together with '&' and appended to the url after '?'.
    """

    def __init__(self, url_base):
        self.opener = urllib2.build_opener(HTTPSGridAuthHandler())
        self.url_base = url_base

    def make_request(self, resource, options = [], method = 'GET', format = 'url'):
        url = self.url_base + '/' + resource
        if method == 'GET' and len(options) != 0:
            url += '?' + '&'.join(options)

        logger.info(url)

        request = urllib2.Request(url)

        if method == 'POST' and len(options):
            if type(options) is list:
                # if it's a list it should be a list of 2-tuples (should be a dict otherwise)
                options = dict(options)
            
            if format == 'url':
                data = urllib.urlencode(options)
            elif format == 'json':
                request.add_header('Content-type', 'application/json')
                data = json.dumps(options)

            request.add_data(data)

        try:
            response = self.opener.open(request)

        except urllib2.HTTPError, e:
            raise

        except urllib2.URLError, e:
            raise

        return response.read()


if __name__ == '__main__':

    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'REST interface')

    parser.add_argument('url_base', metavar = 'URL', help = 'Request URL base.')
    parser.add_argument('resource', metavar = 'RES', help = 'Request resource.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '+', default = [], help = 'Options after ? (chained with &).')
    parser.add_argument('--post', '-P', action = 'store_true', dest = 'use_post', help = 'Use POST instead of GET request.')

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    
    interface = RESTService(args.url_base)

    if args.use_post:
        options = []
        for option in args.options:
            key, eq, value = option.partition('=')
            try:
                opt = next(opt for opt in options if opt[0] == key)
                if type(opt[1]) is list:
                    opt[1].append(value)
                else:
                    options.remove(opt)
                    options.append((key, [opt[1], value]))

            except StopIteration:
                options.append((key, value))

        method = 'POST'
    else:
        options = args.options
        method = 'GET'

    print interface.make_request(args.resource, options, method = method)
