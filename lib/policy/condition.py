import logging

from dynamo.policy.predicates import Predicate

LOG = logging.getLogger(__name__)

class Condition(object):
    """AND-chained Predicates."""

    def __init__(self, text, variables):
        self.text = text
        self.predicates = []
        self.required_attrs = set()

        pred_strs = map(str.strip, text.split(' and '))

        self.time_condition = None

        tmp = ''
        if ' until ' in pred_strs[-1]:
            tmp = pred_strs[-1].split(' until ')
            tmp[-1] = 'until ' + tmp[-1]
        elif ' from ' in pred_strs[-1]:
            tmp = pred_strs[-1].split(' from ')
            tmp[-1] = 'from ' + tmp[-1]
        if tmp != '':
            pred_strs[-1] = tmp[0]             
            self.time_condition = tmp[-1]

        # parsing the individual components
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

            # list of name of attrs
            self.required_attrs.update(variable.required_attrs)

            if len(words) >= 2:
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
        if self.time_condition is not None:
            if 'until' in self.time_condition:
                proc = subprocess.Popen(['date', '-d', self.time_condition.split('until ')[1], '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
                unixt, err = proc.communicate()
                if time.time() > unixt:
                    return False
            else: # from
                proc = subprocess.Popen(['date', '-d', self.time_condition.split('from ')[1], '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
                unixt, err = proc.communicate()
                if time.time() < unixt:
                    return False

        for predicate in self.predicates:
            if not predicate(obj):
                return False

        return True

    def get_variable(self, expr, variables):
        """Return an Attr object using the expr from the given variables dictionary."""

        return variables[expr]
