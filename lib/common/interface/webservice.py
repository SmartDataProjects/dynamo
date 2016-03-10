import urllib2
import httplib
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

    def make_request(self, resource, options = []):
        url = self.url_base + '/' + resource
        if len(options) != 0:
            url += '?' + '&'.join(options)

        logger.info(url)

        request = urllib2.Request(url)

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

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    
    interface = RESTService(args.url_base)

    print interface.make_request(args.resource, args.options)
