import re
import logging

import detox.variables as variables
from detox.predicates import Predicate

logger = logging.getLogger(__name__)

class Condition(object):
    def __init__(self, text):
        self.text = text
        self.predicates = []
        self.used_demand_plugins = set()

        pred_strs = map(str.strip, text.split(' and '))

        for pred_str in pred_strs:
            words = pred_str.split()

            expr = words[0]
            if expr == 'not': # special case for English language
                expr = words[1]
                words[1] = 'not'

            # flags to determine which demand information should be updated
            for plugin, exprs in variables.required_plugins.items():
                if expr in exprs:
                    self.used_demand_plugins.add(plugin)

            try:
                variable = self.get_variable(expr)
            except KeyError:
                raise RuntimeError('Unknown variable ' + expr)

            if len(words) > 2:
                operator = words[1]
            else:
                operator = ''

            rhs_expr = ' '.join(words[2:])

            self.predicates.append(Predicate.get(variable, operator, rhs_expr))

    def __str__(self):
        return self.text

    def match(self, obj):
        for predicate in self.predicates:
            if not predicate(obj):
                return False

        return True

class ReplicaCondition(Condition):
    def get_variable(self, expr):
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        return variables.replica_variables[expr]

    def get_matching_blocks(self, replica):
        """If this is a block-level condition, return the list of matching block replicas."""

        matching_blocks = []
        for block_replica in replica.block_replicas:
            if self.match(block_replica):
                matching_blocks.append(block_replica)

        return matching_blocks

class SiteCondition(Condition):
    def __init__(self, text, partition):
        self.partition = partition

        Condition.__init__(self, text)

    def get_variable(self, expr):
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        variable = variables.site_variables[expr]
        variable.partition = self.partition

        return variable
