import logging

import dynamo.utils.interface as interface

LOG = logging.getLogger(__name__)

class DynamoRegistry(object):
    """
    Registry class. Basically a DB with a web frontend. Class provides access to the frontend as well.
    """

    def __init__(self, config):
        self.host = config.host

        self.frontend = interface.RESTService(url_base = ('https://%s/registry' % self.host))
        self.set_backend(config)

    def set_backend(self, config):
        backend_config = config.config.clone()
        backend_config['host'] = self.host
        self.backend = getattr(config.interface, cls)(backend_config)
