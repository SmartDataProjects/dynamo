import logging

import dynamo.utils.interface as interface

LOG = logging.getLogger(__name__)

class DynamoRegistry(object):
    """
    Registry class. Basically a DB with a web frontend. Class provides access to the frontend as well.
    """

    def __init__(self, config):
        self.set_frontend(config.frontend.interface, config.frontend.config)
        self.set_backend(config.backend.interface, config.backend.config)

    def set_frontend(self, cls, config):
        self.frontend = getattr(interface, cls)(config)

    def set_backend(self, cls, config):
        self.backend = getattr(interface, cls)(config)
