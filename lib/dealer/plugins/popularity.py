import math
import logging

from dealer.plugins._list import plugins
from dealer.plugins.base import BaseHandler
import dealer.configuration as config

logger = logging.getLogger(__name__)

class PopularityHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)
        self._datasets = []

    def get_requests(self, inventory, partition): # override
        self._datasets = []

        for dataset in inventory.datasets.values():
            if dataset.demand.request_weight <= 0.:
                continue

            if dataset.size() * 1.e-12 > config.max_dataset_size:
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

            num_requests = min(config.max_replicas, int(math.ceil(dataset.demand.request_weight / config.request_to_replica_threshold))) - len(dataset.replicas)
            for _ in xrange(num_requests):
                self._datasets.append(dataset)
            
        self._datasets.sort(key = lambda dataset: dataset.demand.request_weight, reverse = True)
        
        return list(self._datasets), [], []

    def save_record(self, run_number, history, copy_list): # override
        history.save_dataset_popularity(run_number, self._datasets)


plugins['Popularity'] = PopularityHandler()

if __name__ == '__main__':
    pass
