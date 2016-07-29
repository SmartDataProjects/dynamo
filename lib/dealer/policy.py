import logging
import collections

import dealer.configuration as dealer_config

logger = logging.getLogger(__name__)

class DealerPolicy(object):
    """
    Defined for each partition and implements the concrete conditions for copies.
    """

    def __init__(self, site_occupancy, partition = '', in_partition = None, group = None, included_sites = None):
        self.site_occupancy = site_occupancy # float(Site)
        self.partition = partition
        if in_partition is None:
            self.in_partition = lambda replica: True
        else:
            self.in_partition = in_partition # bool(DatasetReplica)

        self.group = group

        self.included_sites = included_sites # regexp for site name
