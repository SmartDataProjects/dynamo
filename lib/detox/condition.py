import re
import logging

import detox.variables as variables
from detox.predicates import Predicate, InvalidExpression

logger = logging.getLogger(__name__)

class Condition(object):
    def __init__(self, text):
        self.static = True
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

            # we can optimize execution if all predicates are based on static variables
            if expr in variables.replica_dynamic_variables:
                self.static = False

            # flags to determine which demand information should be updated
            for plugin, exprs in variables.required_plugins.items():
                if expr in exprs:
                    self.used_demand_plugins.add(plugin)

            try:
                vardef = self.get_vardef(expr)
            except KeyError:
                raise InvalidExpression(text)

            if len(words) > 2:
                operator = words[1]
            else:
                operator = ''

            rhs_expr = ' '.join(words[2:])

            # don't hard-code this here - variables.py should also define RHS domain
            if expr == 'dataset.name' and not re.match('/[^/]+/[^/]+/[^/]+', rhs_expr):
                raise ConfigurationError(line)

            self.predicates.append(Predicate.get(vardef, operator, rhs_expr))

    def __str__(self):
        return self.text

    def match(self, obj):
        for predicate in self.predicates:
            if not predicate(obj):
                return False

        return True

class ReplicaCondition(Condition):
    def get_vardef(self, expr):
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        return variables.replica_vardefs[expr]

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

    def get_vardef(self, expr):
        """Return a tuple containing (callable variable definition, variable type, ...)"""

        vardef = variables.site_vardefs[expr]
        if type(vardef[0]) is variables.SiteAttr:
            vardef[0].partition = self.partition

        return vardef
