import sys
import urllib
import urllib2
import httplib
import time
import json
import logging

import common.configuration as config
from common.misc import unicode2str

logger = logging.getLogger(__name__)

GET, POST = range(2) # enumerators

class HTTPSCertKeyHandler(urllib2.HTTPSHandler):
    """
    HTTPS handler authenticating by x509 user key and certificate.
    """

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key = config.webservice.x509_key
        self.cert = self.key

    def https_open(self, req):
        return self.do_open(self.create_connection, req)

    def create_connection(self, host, timeout = 300):
        return httplib.HTTPSConnection(host, key_file = self.key, cert_file = self.cert)


class CERNSSOCookieAuthHandler(urllib2.HTTPSHandler):
    """
    HTTPS handler for CERN single sign-on service. Requires a cookie file
    generated by cern-get-sso-cookie.
    """

    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)

        self.cookies = {}

        with open(config.webservice.cookie_file) as cookie_file:
            # skip the header
            while cookie_file.readline().strip():
                pass

            for line in cookie_file:
                domain, dom_specified, path, secure, expires, name, value = line.split()

                # for some reason important entries are commented out
                if domain.startswith('#'):
                    domain = domain[1:]

                domain = domain.replace('HttpOnly_', '')

                if domain not in self.cookies:
                    self.cookies[domain] = [(name, value)]
                else:
                    self.cookies[domain].append((name, value))

    def https_request(self, request):
        try:
            cookies = self.cookies[request.get_host()]
            # concatenate all cookies for the domain with '; '
            request.add_unredirected_header('Cookie', '; '.join(['%s=%s' % c for c in cookies]))
        except KeyError:
            pass

        return urllib2.HTTPSHandler.https_request(self, request)


class RESTService(object):
    """
    An interface to RESTful APIs (e.g. PhEDEx, DBS) with X509 authentication.
    make_request will take the REST "command" and a list of options as arguments.
    Options are chained together with '&' and appended to the url after '?'.
    Returns python-parsed content.
    """

    def __init__(self, url_base, headers = [], accept = 'application/json'):
        self.url_base = url_base
        self.headers = list(headers)
        self.accept = accept

    def make_request(self, resource = '', options = [], method = GET, format = 'url', auth_handler = HTTPSCertKeyHandler):
        url = self.url_base
        if resource:
            url += '/' + resource

        if method == GET and len(options) != 0:
            if type(options) is list:
                url += '?' + '&'.join(options)
            elif type(options) is str:
                url += '?' + options

        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug(url)
        
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

        wait = 1.
        exceptions = []
        while len(exceptions) != config.webservice.num_attempts:
            try:
                if self.url_base.startswith('https:'):
                    opener = urllib2.build_opener(auth_handler())
                else:
                    opener = urllib2.build_opener(urllib2.HTTPHandler())

                if 'Accept' not in self.headers:
                    opener.addheaders.append(('Accept', self.accept))

                opener.addheaders.extend(self.headers)

                response = opener.open(request)

                # clean up - break reference cycle so python can free the memory up
                for handler in opener.handlers:
                    handler.parent = None
                del opener

                content = response.read()
                del response

                if self.accept == 'application/json':
                    result = json.loads(content)
                    unicode2str(result)

                elif self.accept == 'application/xml':
                    # TODO implement xml -> dict
                    result = content

                del content

                return result
    
            except urllib2.HTTPError as err:
                last_except = (str(err))
            except:
                last_except = sys.exc_info()[:2]

            exceptions.append(last_except)

            logger.info('Exception "%s" occurred in webservice. Trying again in %.1f seconds.', str(last_except), wait)

            time.sleep(wait)
            wait *= 1.5

        else: # exhausted allowed attempts
            logger.error('Too many failed attempts in webservice')
            logger.error('%s' % ' '.join(map(str, exceptions)))
            raise RuntimeError('webservice too many attempts')


if __name__ == '__main__':

    import sys
    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'REST interface')

    parser.add_argument('url_base', metavar = 'URL', help = 'Request URL base.')
    parser.add_argument('resource', metavar = 'RES', help = 'Request resource.')
    parser.add_argument('options', metavar = 'EXPR', nargs = '*', default = [], help = 'Options after ? (chained with &).')
    parser.add_argument('--post', '-P', action = 'store_true', dest = 'use_post', help = 'Use POST instead of GET request.')
    parser.add_argument('--output-format', '-f', metavar = 'FORMAT', dest = 'output_format', default = 'json', help = 'json or xml')
    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')
    parser.add_argument('--cert', '-c', metavar = 'PATH', dest = 'certificate', default = '', help = 'Use non-default certificate.')

    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    if args.output_format == 'json':
        accept = 'application/json'
    elif args.output_format == 'xml':
        accept = 'application/xml'
    else:
        logging.error('Unrecognized format %s', args.output_format)
        sys.exit(1)

    if args.certificate:
        config.webservice.x509_key = args.certificate

    interface = RESTService(args.url_base, accept = accept)

    if args.use_post:
        method = POST
    else:
        method = GET

    print interface.make_request(args.resource, args.options, method = method)
