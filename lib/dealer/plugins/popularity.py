import math
import logging

from dealer.plugins._list import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config

logger = logging.getLogger(__name__)

class PopularityHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

        self.name = 'Popularity'
        self._datasets = []

    def get_requests(self, inventory, partition): # override
        self._datasets = []
        requests = []

        for dataset in inventory.datasets.values():
            if dataset.replicas is None:
                # this dataset has no replica in the pool to begin with
                continue

            if dataset.demand.request_weight <= 0.:
                continue

            if dataset.size * 1.e-12 > config.max_dataset_size:
                continue

            for replica in dataset.replicas:
                for block_replica in replica.block_replicas:
                    if partition(block_replica):
                        break
                else:
                    # no block replica in partition
                    continue

                # this replica is (partially) in partition
                break

            else: # no block in partition
                continue

            self._datasets.append(dataset)

            num_requests = min(config.max_replicas, int(math.ceil(dataset.demand.request_weight / config.request_to_replica_threshold))) - len(dataset.replicas)
            if num_requests <= 0:
                continue

            requests.append((dataset, num_requests))
            
        requests.sort(key = lambda x: x[0].demand.request_weight, reverse = True)

        datasets_to_request = []

        # [(d1, n1), (d2, n2), ...] -> [d1, d2, .., d1, ..] (d1 repeats n1 times)
        while True:
            added_request = False
            for ir in xrange(len(requests)):
                dataset, num_requests = requests[ir]
                if num_requests == 0:
                    continue

                datasets_to_request.append(dataset)
                requests[ir] = (dataset, num_requests - 1)
                added_request = True

            if not added_request:
                break
        
        return datasets_to_request, [], []

    def save_record(self, run_number, history, copy_list): # override
        history.save_dataset_popularity(run_number, self._datasets)


plugins['Popularity'] = PopularityHandler()
