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

    def __init__(self, default, rules, quotas, partition = '', site_requirement = None, replica_requirement = None):
        self.default_decision = default # decision
        self.rules = rules # [rule]
        self.quotas = quotas # {site: quota}
        self.partition = partition
        # bool(site, partition, initial). initial: check deletion should be triggered.
        self.site_requirement = site_requirement
        # An object with two methods dataset = int(DatasetReplica), block = bool(BlockReplica).
        # dataset return values: 1->drep is in partition, 0->drep is not in partition, -1->drep is partially in partition
        self.replica_requirement = replica_requirement
        self.untracked_replicas = {} # temporary container of block replicas that are not in the partition

    def partition_replicas(self, datasets):
        """
        Take the full list of datasets and pick out block replicas that are not in the partition.
        If a dataset replica loses all block replicas, take the dataset replica itself out of inventory.
        Return the list of all dataset replicas in the partition.
        """

        all_replicas = []

        if self.replica_requirement is None:
            # all replicas are in
            for dataset in datasets:
                all_replicas.extend(dataset.replicas)
                
            return all_replicas

        # otherwise sort replicas out

        # stacking up replicas (rather than removing them one by one) for efficiency
        site_all_dataset_replicas = collections.defaultdict(list)
        site_all_block_replicas = collections.defaultdict(list)

        for dataset in datasets:
            ir = 0
            while ir != len(dataset.replicas):
                replica = dataset.replicas[ir]
                site = replica.site

                partitioning = self.replica_requirement.dataset(replica)

                if partitioning > 0:
                    # this replica is fully in partition
                    site_all_dataset_replicas[site].append(replica)
                    site_all_block_replicas[site].extend(replica.block_replicas)

                elif partitioning == 0:
                    # this replica is completely not in partition
                    self.untracked_replicas[replica] = replica.block_replicas
                    replica.block_replicas = []

                else:
                    # this replica is partially in partition
                    site_all_dataset_replicas[site].append(replica)

                    site_block_replicas = site_all_block_replicas[site]

                    block_replicas = []
                    not_in_partition = []
                    for block_replica in replica.block_replicas:
                        if self.replica_requirement.block(block_replica):
                            site_block_replicas.append(block_replica)
                            block_replicas.append(block_replica)
                        else:
                            not_in_partition.append(block_replica)

                    replica.block_replicas = block_replicas
    
                    if len(not_in_partition) != 0:
                        self.untracked_replicas[replica] = not_in_partition

                if len(replica.block_replicas) == 0:
                    dataset.replicas.pop(ir)
                else:
                    all_replicas.append(replica)
                    ir += 1

        for site, dataset_replicas in site_all_dataset_replicas.items():
            site.dataset_replicas = dataset_replicas

        for site, block_replicas in site_all_block_replicas.items():
            site.set_block_replicas(block_replicas)

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

    def evaluate(self, replica, dataset_demand):
        for rule in self.rules:
            result = rule(replica, dataset_demand)
            if result is not None:
                break
        else:
            return replica, self.default_decision, 'Policy default'

        return result

    def sort_deletion_candidates(self, replicas_demands):
        """
        Rank and sort replicas in decreasing order of deletion priority.
        """

        return sorted(replicas_demands, key = lambda (r, d): d.global_usage_rank, reverse = True)
