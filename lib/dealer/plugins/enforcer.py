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

        self.interface = EnforcerInterface(config.enforcer)

    def get_requests(self, inventory, history, policy): # override
        requests = []
        for dataset, site in self.interface.report_back(inventory):
            requests.append(DealerRequest(dataset, destination = site))

        return requests
