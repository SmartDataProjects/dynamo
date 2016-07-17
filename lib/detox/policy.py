import logging
import collections

logger = logging.getLogger(__name__)

class Policy(object):
    """
    Responsible for partitioning the replicas, setting quotas and activating deletion on sites, and making deletion decisions on replicas.
    The core of the object is a stack of rules (specific rules first) with a fall-back default decision.
    A rule is a callable object with (replica, demand_manager) as arguments that returns None or (replica, decision, reason)
    """

    # do not change order - used by history records
    DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(1, 4)
    DECISION_STR = {DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}

    def __init__(self, default, rules, quotas, partition = '', site_requirement = None, block_requirement = None):
        self.default_decision = default # decision
        self.rules = rules # [rule]
        self.quotas = quotas # {site: quota}
        self.partition = partition
        # bool(site, partition, initial). initial: check deletion should be triggered.
        self.site_requirement = site_requirement
        # bool(block_replica). True if block replica is in the partition.
        self.block_requirement = block_requirement
        self.untracked_replicas = {} # temporary container of block replicas that are not in the partition

    def partition_replicas(self, datasets):
        """
        Take the full list of datasets and pick out block replicas that are not in the partition.
        If a dataset replica loses all block replicas, take the dataset replica itself out of inventory.
        Return the list of all dataset replicas in the partition.
        """

        all_replicas = []

        if self.block_requirement is None:
            # all blocks are in
            for dataset in datasets:
                all_replicas.extend(dataset.replicas)
                
            return all_replicas

        # otherwise sort replicas out
        for dataset in datasets:
            ir = 0
            while ir != len(dataset.replicas):
                replica = dataset.replicas[ir]

                not_in_partition = []
                for block_replica in replica.block_replicas:
                    if not self.block_requirement(block_replica):
                        not_in_partition.append(block_replica)

                if len(not_in_partition) != 0:
                    self.untracked_replicas[replica] = not_in_partition
                    for block_replica in not_in_partition:
                        replica.block_replicas.remove(block_replica)
                        replica.site.remove_block_replica(block_replica)

                if len(replica.block_replicas) == 0:
                    dataset.replicas.pop(ir)
                    replica.site.dataset_replicas.remove(replica)
                else:
                    all_replicas.append(replica)
                    ir += 1

        return all_replicas

    def restore_replicas(self):
        while len(self.untracked_replicas) != 0:
            replica, block_replicas = self.untracked_replicas.popitem()

            dataset = replica.dataset
            site = replica.site

            if replica not in dataset.replicas:
                dataset.replicas.append(replica)

            if replica not in site.dataset_replicas:
                site.dataset_replicas.append(replica)

            for block_replica in block_replicas:
                replica.block_replicas.append(block_replica)
                site.add_block_replica(block_replica)

    def need_deletion(self, site, initial = False):
        if self.site_requirement is None:
            return True
        else:
            return self.site_requirement(site, self.partition, initial)

    def evaluate(self, replica, demand_manager):
        for rule in self.rules:
            result = rule(replica, demand_manager)
            if result is not None:
                break
        else:
            return replica, self.default_decision, 'Policy default'

        return result

    def sort_deletion_candidates(self, replicas, demands):
        """
        Rank and sort replicas in decreasing order of deletion priority.
        """

        return sorted(replicas, key = lambda r: demands.dataset_demands[r.dataset].global_usage_rank, reverse = True)
