import logging
import collections
import random
import time

import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

random.seed(time.time())

class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, partition, group = None, version = ''):
        self.partition = partition
        self.group = group
        self.version = version
        self.target_site_def = None
        
        self._request_plugins = []

    def add_plugin(self, plugin, priority = 1):
        """
        Add a plugin with a priority factor. Request from a plugin is picked up
        at a rate proportional to the inverse of the priority factor.
        Algorithm:
        For n plugins with priority factors p_i (i=0,...,n-1),
        P = sum_i(1/p_i)
        R_i = (1/p_i) / P
        
        @param plugin    Dealer plugin object
        @param priority  Priority factor
        """

        if priority < 1:
            raise RuntimeError('Invalid priority factor')

        self._request_plugins.append((plugin, priority))

    def collect_requests(self, inventory):
        """
        Collect requests from each plugin and return a prioritized list
        """

        datasets = []
        blocks = []
        files = []

        for plugin, priority in self._request_plugins:
            d, b, f = plugin.get_requests(inventory, self.partition)
            if len(d):
                logger.debug('%s requesting %d datasets', plugin.name, len(d))
                datasets.append((d, 1. / priority))
            if len(b):
                logger.debug('%s requesting %d blocks', plugin.name, len(b))
                blocks.append((b, 1. / priority))
            if len(f):
                logger.debug('%s requesting %d files', plugin.name, len(f))
                files.append((f, 1. / priority))

        items = ([], [], [])

        for il, itemlist in enumerate([datasets, blocks, files]):
            while True:
                total = sum(p for l, p in itemlist)
                if total == 0.:
                    break

                x = random.uniform(0., total)
                t = 0.
                i = 0
                while i < len(itemlist):
                    t += itemlist[i][1]
                    if t >= x:
                        l = itemlist[i][0]
                        items[il].append(l.pop(0))
                        break
    
                    i += 1
    
                if len(l) == 0:
                    itemlist.pop(i)

        return items

    def record(self, run_number, history, copy_list):
        for plugin, priority in self._request_plugins:
            plugin.save_record(run_number, history, copy_list)
