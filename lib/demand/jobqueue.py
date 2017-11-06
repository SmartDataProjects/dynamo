import time
import math

import common.configuration as config

class JobQueue(object):
    """
    A plugin for DemandManager that computes "request weights" for datasets based on the number of batch jobs requesting them.
    Sets one demand value:
      request_weight:  float value
    """

    def __init__(self):
        self._last_update = 0 # a datetime.date object when loaded
        self._request_list = {}

    def load(self, inventory):
        records = inventory.store.load_dataset_requests(inventory.datasets.values())
        self._last_update = records[0]

        self._compute(records[1])

    def update(self, inventory):
        pass

    def _compute(self, request_list):
        """
        Set the dataset request weight based on request list. Formula:
          w = Sum(exp(-t_i/T))
        where t_i is the time distance of the ith request from now. T is defined in the configuration.
        """

        now = time.time()
        decay_constant = config.jobqueue.weight_halflife * 3600. * 24. / math.log(2.)

        for dataset, requests in request_list.items():
            weight = 0.
            for reqdata in requests.values():
                # first element of reqdata tuple is the queue time
                weight += math.exp((reqdata[0] - now) / decay_constant)
            
            dataset.demand['request_weight'] = weight
