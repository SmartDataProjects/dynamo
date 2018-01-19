import logging

from webservice import RESTService, GET

LOG = logging.getLogger(__name__)

class SiteStatusBoard(RESTService):
    def __init__(self, config):
        RESTService.__init__(self, config)

    def make_request(self, resource = '', options = [], method = GET, format = 'url', retry_on_error = True): #override
        """
        Strip the "header" and return the body JSON.
        """

        response = RESTService.make_request(self, resource, options = options, method = method, format = format, retry_on_error = retry_on_error)

        try:
            result = response['csvdata']
        except KeyError:
            LOG.error(response)
            return

        return result
