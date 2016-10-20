import logging

from detox.variables import replica_vardefs, replica_dynamic_variables, replica_access_variables, replica_request_variables, replica_lock_variables, site_vardefs
from detox.predicates import Predicate, InvalidExpression

logger = logging.getLogger(__name__)

class Condition(object):
    def __init__(self, text):
        self.static = True
        self.uses_accesses = False
        self.uses_requests = False
        self.uses_locks = False
        self.text = text
        self.predicates = []

        pred_strs = map(str.strip, text.split(' and '))

        for pred_str in pred_strs:
            words = pred_str.split()

            expr = words[0]
            if expr == 'not': # special case for English language
                expr = words[1]
                words[1] = 'not'

            # we can optimize execution if all predicates are based on static variables
            if expr in replica_dynamic_variables:
                self.static = False

            # flags to determine which demand information should be updated
            if expr in replica_access_variables:
                self.uses_accesses = True
            if expr in replica_request_variables:
                self.uses_requests = True
            if expr in replica_lock_variables:
                self.uses_locks = True

            try:
                vardef = self.get_vardef(expr)
            except KeyError:
                raise InvalidExpression(text)

            if len(words) > 2:
                operator = words[1]
            else:
                operator = ''

            rhs_expr = ' '.join(words[2:])

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
        return replica_vardefs[expr]
        
class SiteCondition(Condition):
    def __init__(self, text, partition):
        self.partition = partition

        Condition.__init__(self, text)

    def get_vardef(self, expr):
        vardef = site_vardefs[expr]
        return (vardef[0](self.partition),) + vardef[1:]
