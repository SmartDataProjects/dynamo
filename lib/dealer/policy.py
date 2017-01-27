import logging
import collections

import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, partition, group = None, version = ''):
        self.partition = partition
        self.group = group
        self.version = version
        self.target_site_def = None
        
        self.request_plugins = []

    def collect_requests(self, inventory):
        requests = {}

        for plugin in self.request_plugins:
            d, b, f = plugin.get_requests(inventory, self.partition)
            requests[plugin] = (d, b, f)

        return requests

    def record(self, run_number, history, copy_list):
        for plugin in self.request_plugins:
            plugin.save_record(run_number, history, copy_list)
