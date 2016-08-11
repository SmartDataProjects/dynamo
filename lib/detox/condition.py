import logging

from detox.variables import replica_vardefs, site_vardefs
from detox.predicates import Predicate, InvalidExpression

logger = logging.getLogger(__name__)

class Condition(object):
    def __init__(self, text):
        self.predicates = []

        pred_strs = text.split(' and ')

        for pred_str in pred_strs:
            words = pred_str.split()

            expr = words[0]
            if expr == 'not': # special case for English language
                expr = words[1]
                words[1] = 'not'

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
