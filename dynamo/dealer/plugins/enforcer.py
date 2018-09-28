import logging
import re
import fnmatch
import random

from base import BaseHandler, DealerRequest
from dynamo.dataformat import Configuration
from dynamo.enforcer.interface import EnforcerInterface

class EnforcerHandler(BaseHandler):
    """
    Request replication of datasets using custom rules and destinations.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Enforcer')
        
        if type(config.enforcer) is str:
            # A path to the common enforcer configuration
            enforcer_config = Configuration(config.enforcer)
        else:
            enforcer_config = config.enforcer

        self.interface = EnforcerInterface(enforcer_config)

    def get_requests(self, inventory, policy): # override
        requests = []
        for dataset, site in self.interface.report_back(inventory):
            requests.append(DealerRequest(dataset, destination = site))

        return requests
