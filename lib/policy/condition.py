from policy.predicates import Predicate

class Condition(object):
    def __init__(self, text, variables):
        self.text = text
        self.predicates = []
        self.demand_classes = set()

        pred_strs = map(str.strip, text.split(' and '))

        for pred_str in pred_strs:
            words = pred_str.split()

            expr = words[0]
            if expr == 'not': # special case for English language
                expr = words[1]
                words[1] = 'not'

            try:
                variable = self.get_variable(expr, variables)
            except KeyError:
                raise RuntimeError('Unknown variable ' + expr)

            # list of name of demand plugins
            self.demand_classes.update(variable.source)

            if len(words) > 2:
                operator = words[1]
            else:
                operator = ''

            rhs_expr = ' '.join(words[2:])

            self.predicates.append(Predicate.get(variable, operator, rhs_expr))

    def __str__(self):
        return 'Condition \'%s\'' % self.text

    def __repr__(self):
        return 'Condition(\'%s\')' % self.text

    def match(self, obj):
        for predicate in self.predicates:
            if not predicate(obj):
                return False

        return True

    def get_variable(self, expr, variables):
        """Return an Attr object using the expr from the given variables dictionary."""

        return variables[expr]
