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
