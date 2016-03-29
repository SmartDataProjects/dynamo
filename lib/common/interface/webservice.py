import urllib
import urllib2
import httplib
import json
import logging

import common.configuration as config

logger = logging.getLogger(__name__)

GET, POST = range(2) # enumerators

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

    def make_request(self, resource, options = [], method = GET, format = 'url'):
        url = self.url_base + '/' + resource
        if method == GET and len(options) != 0:
            if type(options) is list:
                url += '?' + '&'.join(options)
            elif type(options) is str:
                url += '?' + options

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(url)
        else:
            logger.info(self.url_base + '/' + resource)
        
        request = urllib2.Request(url)

        if method == POST and len(options) != 0:
            if type(options) is list:
                # convert key=value strings to (key, value) 2-tuples
                optlist = []
                for opt in options:
                    if type(opt) is tuple:
                        optlist.append(opt)

                    elif type(opt) is str:
                        key, eq, value = opt.partition('=')
                        if eq == '=':
                            optlist.append((key, value))

                options = optlist
            
            if format == 'url':
                # Options can be a dict or a list of 2-tuples. The latter case allows repeated keys (e.g. dataset=A&dataset=B)
                data = urllib.urlencode(options)

            elif format == 'json':
                # Options can be a dict or a list of 2-tuples. Repeated keys in the list case gets collapsed.
                if type(options) is list:
                    optdict = {}
                    for key, value in options:
                        if key in optdict:
                            try:
                                optdict[key].append(value)
                            except AttributeError:
                                current = optdict[key]
                                optdict[key] = [current, value]
                        else:
                            optdict[key] = value
    
                    options = optdict

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
        method = POST
    else:
        method = GET

    print interface.make_request(args.resource, args.options, method = method)
