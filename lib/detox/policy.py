import re
import fnmatch
import logging
import collections
import subprocess

from detox.variables import replica_vardefs
from detox.condition import ReplicaCondition, SiteCondition

logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    def __init__(self, *args):
        if len(args) != 0:
            self.str = args[0] % args[1:]
        else:
            self.str = ''

    def __str__(self):
        return repr(self.str)

class PolicyLine(object):
    """
    Call this Policy when fixing the terminology.
    AND-chained list of predicates.
    """

    def __init__(self, condition, decision, text):
        self.condition = condition
        self.decision = decision
        self.text = text
        self.has_match = False

    def __str__(self):
        return self.text

    def __call__(self, replica):
        if self.condition.match(replica):
            self.has_match = True
            return replica, self.decision, self.text

class Policy(object):
    """
    Responsible for partitioning the replicas, setting quotas and activating deletion on sites, and making deletion decisions on replicas.
    The core of the object is a stack of rules (specific rules first) with a fall-back default decision.
    A rule is a callable object that takes a dataset replica as an argument and returns None or (replica, decision, reason)
    """

    # do not change order - used by history records
    DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(1, 4)
    DECISION_STR = {DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}
    ST_ITERATIVE, ST_STATIC, ST_GREEDY = range(3)

    def __init__(self, partition, quotas, partitioning, lines):
        self.quotas = quotas # {site: quota}
        self.partition = partition
        # An object with two methods dataset = int(DatasetReplica), block = bool(BlockReplica).
        # dataset return values: 1->drep is in partition, 0->drep is not in partition, -1->drep is partially in partition
        self.partitioning = partitioning
        self.untracked_replicas = {} # temporary container of block replicas that are not in the partition

        self.parse_rules(lines)

    def parse_rules(self, lines):
        if type(lines) is file:
            conf = lines
            lines = map(str.strip, conf.read().split('\n'))
            il = 0
            while il != len(lines):
                if lines[il] == '' or lines[il].startswith('#'):
                    lines.pop(il)
                else:
                    il += 1

        self.target_site_def = None
        self.deletion_trigger = None
        self.stop_condition = None
        self.rules = []
        self.default_decision = -1
        self.strategy = -1
        self.candidate_sort = None

        LINE_SITE_TARGET, LINE_DELETION_TRIGGER, LINE_STOP_CONDITION, LINE_POLICY, LINE_STRATEGY, LINE_ORDER = range(6)

        for line in lines:
            line_type = -1

            words = line.split()
            if words[0] == 'On':
                line_type = LINE_SITE_TARGET
            elif words[0] == 'When':
                line_type = LINE_DELETION_TRIGGER
            elif words[0] == 'Until':
                line_type = LINE_STOP_CONDITION
            elif words[0] == 'Strategy':
                line_type = LINE_STRATEGY
            elif words[0] == 'Order':
                line_type = LINE_ORDER
            elif words[0] == 'Protect':
                decision = Policy.DEC_PROTECT
                line_type = LINE_POLICY
            elif words[0] == 'Delete':
                decision = Policy.DEC_DELETE
                line_type = LINE_POLICY
            else:
                raise ConfigurationError(line)

            if len(words) == 1:
                if line_type == LINE_POLICY:
                    self.default_decision = decision
                    continue
                else:
                    raise ConfigurationError(line)

            if line_type == LINE_STRATEGY:
                self.strategy = eval('Policy.ST_' + words[1].upper())

            elif line_type == LINE_ORDER:
                if words[1] == 'increasing':
                    reverse = False
                elif words[1] == 'decreasing':
                    reverse = True
                else:
                    raise ConfigurationError(words[1])

                sortkey = replica_vardefs[words[2]][0]
                self.candidate_sort = lambda replicas: sorted(replicas, key = sortkey, reverse = reverse)

            else:
                cond_text = ' '.join(words[1:])

                if line_type == LINE_SITE_TARGET:
                    self.target_site_def = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_DELETION_TRIGGER:
                    self.deletion_trigger = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_STOP_CONDITION:
                    self.stop_condition = SiteCondition(cond_text, self.partition)

                elif line_type == LINE_POLICY:
                    self.rules.append(PolicyLine(ReplicaCondition(cond_text), decision, cond_text))

        if self.target_site_def is None:
            raise ConfigurationError('Target site definition missing.')
        if self.deletion_trigger is None or self.stop_condition is None:
            raise ConfigurationError('Deletion trigger and release expressions are missing.')
        if self.default_decision == -1:
            raise ConfigurationError('Default decision not given.')
        if self.strategy == -1:
            raise ConfiguraitonError('Strategy is not specified.')
        if self.candidate_sort is None:
            raise ConfiguraitonError('Deletion candidate sorting is not specified.')

    def partition_replicas(self, datasets):
        """
        Take the full list of datasets and pick out block replicas that are not in the partition.
        If a dataset replica loses all block replicas, take the dataset replica itself out of inventory.
        Return the list of all dataset replicas in the partition.
        """

        all_replicas = []

        if self.partitioning is None:
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

                partitioning = self.partitioning.dataset(replica)

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
                        if self.partitioning.block(block_replica):
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

    def no_more_deletion(self, site):
        return self.stop_condition.match(site)

    def evaluate(self, replica):
        for rule in self.rules:
            result = rule(replica)
            if result is not None:
                break
        else:
            return replica, self.default_decision, 'Policy default'

        return result

    def sort_deletion_candidates(self, replicas):
        return self.candidate_sort(replicas)
