import logging
import re
import fnmatch
import random

from base import BaseHandler
from dynamo.dataformat import Configuration
from dynamo.enforcer import EnforcerInterface

LOG = logging.getLogger(__name__)

class EnforcerHandler(BaseHandler):
    """
    Request replication of datasets using custom rules and destinations.
    """

    def __init__(self, config):
        BaseHandler.__init__(self, 'Enforcer')

        self.policy = Configuration(config.policy)
        self.max_dataset_size = config.max_dataset_size * 1.e+12

    def get_requests(self, inventory, history, policy): # override
        requests = []
        rrd_info = []

        partition = inventory.partitions[policy.partition_name]

        write_rrds = False

        enforcer_instance = EnforcerInterface(write_rrds, self.max_dataset_size)
        requests = enforcer_instance.report_back(inventory, self.policy, partition)

        return requests
