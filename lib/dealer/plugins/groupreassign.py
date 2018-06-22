import logging

from base import BaseHandler, DealerRequest

LOG = logging.getLogger(__name__)

class GroupReassigner(BaseHandler):
    def __init__(self, config):
        BaseHandler.__init__(self, 'GroupReassigner')
        
        self.from_groups = list(config.from_groups)
    
    def get_requests(self, inventory, policy): # override
        from_groups = set(inventory.groups[g] for g in self.from_groups)

        partition = inventory.partitions[policy.partition_name]

        requests = []

        for site in inventory.sites.itervalues():
            sitepartition = site.partitions[partition]

            for dataset_replica, block_replicas in sitepartition.replicas.iteritems():
                if block_replicas is None:
                    block_replicas = dataset_replica.block_replicas

                for block_replica in block_replicas:
                    if block_replica.group in from_groups:
                        break

                else:
                    # no match
                    continue

                dataset = dataset_replica.dataset
                blocks = set(r.block for r in block_replicas)
                if blocks == dataset.blocks:
                    requests.append(DealerRequest(dataset, destination = site))
                else:
                    requests.append(DealerRequest(list(blocks), destination = site))

        return requests
