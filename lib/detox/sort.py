from dataformat import ConfigurationError
import policy.variables as variables
from policy.attrs import Attr

class SortKey(object):
    """
    Used for sorting replicas.
    """
    def __init__(self, text):
        self.vars = []
        # Set of attr names used by variables used in sort
        self.required_attrs = set()

        words = text.split()
        iw = 0
        while iw < len(words):
            direction = words[iw]
            if direction == 'none':
                break
            elif direction == 'increasing':
                reverse = False
            elif direction == 'decreasing':
                reverse = True
            else:
                raise ConfigurationError('Invalid sorting order: ' + words[iw])

            varname = words[iw + 1]
            iw += 2

            variable = variables.replica_variables[varname]
            if variable.vtype != Attr.NUMERIC_TYPE and variable.vtype != Attr.TIME_TYPE:
                raise ConfigurationError('Cannot use non-numeric type to sort: ' + varname)

            self.required_attrs.update(variable.required_attrs)

            self.vars.append((variable, reverse))

    def __call__(self, replica):
        key = tuple()
        for var, reverse in self.vars:
            if reverse:
                key += (-var.get(replica),)
            else:
                key += (var.get(replica),)

        return key
