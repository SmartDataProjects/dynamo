import logging

LOG = logging.getLogger(__name__)

class BlockReplicaRelativeAge(object):
    """
    Sets one attr:
      blockreplica_relative_age - Relative time (s) of the block replica last_update to the latest in the partition
    """

    produces = ['blockreplica_relative_age']

    def __init__(self, config):
        pass

    def load(self, inventory):
        latest = 0
        for dataset in inventory.datasets.itervalues():
            for replica in dataset.replicas:
                for block_replica in replica.blockreplicas:
                    if block_replica.last_update > latest:
                        latest = block_replica.last_update

        for dataset in inventory.datasets.itervalues():
            time_map = {}
            for replica in dataset.replicas:
                for block_replica in replica.blockreplicas:
                    time_map[block_replica] = latest - block_replica.last_update

            dataset.attr['blockreplica_relative_age'] = time_map
