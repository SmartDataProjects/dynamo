import re
import fnmatch
import logging
import collections
import subprocess

import detox.policies.expressions as expressions

logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    pass

class BinaryExpr(object):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.rhs = rhs
        if op == '==':
            self._call = lambda r, d: self.lhs(r, d) == self.rhs
        elif op == '!=':
            self._call = lambda r, d: self.lhs(r, d) != self.rhs
        elif op == '<':
            self._call = lambda r, d: self.lhs(r, d) < self.rhs
        elif op == '>':
            self._call = lambda r, d: self.lhs(r, d) > self.rhs
        else:
            logger.error('Unknown operator %s', op)
            raise ConfigurationError()

    def __call__(self, r, d):
        return self._call(r, d)

class PatternMatch(object):
    def __init__(self, lhs, pattern, match):
        if '*' in pattern:
            self.pattern = re.compile(fnmatch.translate(pattern))
            if match:
                self._call = lambda r, d: self.pattern.match(lhs(r, d)) is not None
            else:
                self._call = lambda r, d: self.pattern.match(lhs(r, d)) is None

        else:
            self.pattern = pattern
            if match:
                self._call = lambda r, d: lhs(r, d) == self.pattern
            else:
                self._call = lambda r, d: lhs(r, d) != self.pattern

    def __call__(self, r, d):
        return self._call(r, d)

class PolicyLine(object):
    """
    Call this Policy when fixing the terminology.
    AND-chained list of predicates.
    """

    def __init__(self, decision, text):
        self.predicates = []
        self.decision = decision
        self.text = text

    def add_predicate(self, pred):
        self.predicates.append(pred)

    def __call__(self, replica, dataset_demand):
        for pred in self.predicates:
            if not pred(replica, dataset_demand):
                return
            
        return replica, self.decision, self.text

class Policy(object):
    """
    Responsible for partitioning the replicas, setting quotas and activating deletion on sites, and making deletion decisions on replicas.
    The core of the object is a stack of rules (specific rules first) with a fall-back default decision.
    A rule is a callable object with (replica, demand_manager) as arguments that returns None or (replica, decision, reason)
    """

    # do not change order - used by history records
    DEC_DELETE, DEC_KEEP, DEC_PROTECT = range(1, 4)
    DECISION_STR = {DEC_DELETE: 'DELETE', DEC_KEEP: 'KEEP', DEC_PROTECT: 'PROTECT'}
    ST_ITERATIVE, ST_STATIC, ST_GREEDY = range(3)

    @staticmethod
    def parse_rules(lines):
        rules = []
        default_decision = None

        for line in lines:
            if default_decision is not None:
                logger.error('Invalid policy lines after default decision.')
                raise ConfigurationError()

            words = line.split()
            if words[0] == 'Protect':
                decision = Policy.DEC_PROTECT
            else:
                decision = Policy.DEC_DELETE

            if len(words) == 1:
                default_decision = decision
                continue

            if words[1] != 'if':
                logger.error('Invalid policy line %s', line)
                raise ConfigurationError()

            condition = ' '.join(words[2:])

            policy_line = PolicyLine(decision, condition)

            predicates = condition.split(' and ')

            for predicate in predicates:
                words = predicate.split()

                expr = words[0]

                try:
                    expr_def = expressions.expressions[expr]
                except KeyError:
                    logger.error('Invalid expression %s', expr)
                    raise ConfigurationError()

                varmap, vtype = expr_def[:2]

                if vtype == expressions.BOOL_TYPE:
                    policy_line.add_predicate(varmap)
                    if len(words) > 1:
                        logger.error('Invalid bool-type expression %s', predicate)
                        raise ConfigurationError()

                    continue

                if len(words) == 1:
                    logger.error('RHS for expression %s missing', expr)
                    raise ConfigurationError()

                operator = words[1]

                if vtype == expressions.NUMERIC_TYPE:
                    if len(expr_def) > 2:
                        rhs = expr_def[2](words[2])
                    else:
                        rhs = float(words[2])

                    policy_line.add_predicate(BinaryExpr(varmap, operator, rhs))

                elif vtype == expressions.TEXT_TYPE:
                    if operator == '==':
                        match = True
                    elif operator == '!=':
                        match = False
                    else:
                        logger.error('Invalid operator for TEXT_TYPE: %s', operator)
                        raise ConfigurationError()

                    policy_line.add_predicate(PatternMatch(varmap, words[2], match))
                
                elif vtype == expressions.TIME_TYPE:
                    rhs_expr = ' '.join(words[2:])
                    proc = subprocess.Popen(['date', '-d', rhs_expr, '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
                    out, err = proc.communicate()
                    if err != '':
                        logger.error('Invalid time expression %s', rhs_expr)
                        raise ConfigurationError()

                    try:
                        rhs = float(out.strip())
                    except:
                        logger.error('Invalid time expression %s', rhs_expr)
                        raise ConfigurationError()

                    policy_line.add_predicate(BinaryExpr(varmap, operator, rhs))

            rules.append(policy_line)

        if default_decision is None:
            logger.error('Default decision not given.')
            raise ConfigurationError()

        return default_decision, rules

    def __init__(self, default, rules, strategy, quotas, partition = '', site_requirement = None, replica_requirement = None, candidate_sort = None):
        self.default_decision = default # decision
        self.rules = rules # [rule]
        self.strategy = strategy # one of ST_ enums
        self.quotas = quotas # {site: quota}
        self.partition = partition
        # bool(site, partition, initial). initial: check deletion should be triggered.
        self.site_requirement = site_requirement
        # An object with two methods dataset = int(DatasetReplica), block = bool(BlockReplica).
        # dataset return values: 1->drep is in partition, 0->drep is not in partition, -1->drep is partially in partition
        self.replica_requirement = replica_requirement
        self.untracked_replicas = {} # temporary container of block replicas that are not in the partition
        # sorted_list_of_replicas(list_of_(replica, demand))
        if candidate_sort is None:
            self.candidate_sort = lambda r_d: [r for r, d in sorted(r_d, key = lambda (r, d): d.global_usage_rank)]
        else:
            self.candidate_sort = candidate_sort

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
        return self.candidate_sort(replicas_demands)
