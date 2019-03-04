import re

import dynamo.policy.attrs as attrs

class InvalidOperator(Exception):
    pass

##################
## Base classes ##
##################

class Predicate(object):
    @staticmethod
    def get(variable, op = '', rhs_expr = ''):
        if op in UnaryExpr.operators:
            if rhs_expr != '':
                raise InvalidOperator(op)
            return UnaryExpr.get(variable, op)

        elif op in BinaryExpr.operators:
            if rhs_expr == '':
                raise InvalidOperator(op)
            return BinaryExpr.get(variable, op, rhs_expr)

        elif op in SetElementExpr.operators:
            if rhs_expr == '':
                raise InvalidOperator(op)
            return SetElementExpr.get(variable, op, rhs_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, variable):
        self.variable = variable

    def __call__(self, obj):
        """
        Call _eval of the inherited classes.
        In case the LHS is a container (can happen when evaluating a block-level
        expression over a dataset replica), return the OR of _eval calls over the
        container elements.
        """

        lhs = self.variable.get(obj)

        # first check for strings - strings are iterable
        if isinstance(lhs, basestring):
            pass
        else:
            try:
                # LHS may be a container
                # Then we return the result of OR over all elements
                for l in lhs:
                    if self._eval(l):
                        return True

                return False

            except TypeError:
                pass

        return self._eval(lhs)

class UnaryExpr(Predicate):
    operators = ['', 'not']

    @staticmethod
    def get(variable, op):
        if op == '':
            return Assert(variable)
        elif op == 'not':
            return Negate(variable)

    def __init__(self, variable):
        Predicate.__init__(self, variable)

        if self.variable.vtype != attrs.Attr.BOOL_TYPE:
            raise InvalidOperator(op)

class BinaryExpr(Predicate):
    operators = ['==', '!=', '=~', '!=~', '<', '>', 'older_than', 'newer_than']

    @staticmethod
    def get(variable, op, rhs_expr):
        if op == '==':
            return Eq(variable, rhs_expr)
        elif op == '!=':
            return Neq(variable, rhs_expr)
        elif op == '=~':
            return Eq(variable, rhs_expr, is_re = True)
        elif op == '!=~':
            return Neq(variable, rhs_expr, is_re = True)
        elif op == '<' or op == 'older_than':
            return Lt(variable, rhs_expr)
        elif op == '>' or op == 'newer_than':
            return Gt(variable, rhs_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, variable, rhs_expr, is_re = False):
        Predicate.__init__(self, variable)

        self.rhs = self.variable.rhs_map(rhs_expr, is_re = is_re)

class SetElementExpr(Predicate):
    operators = ['in', 'notin']

    @staticmethod
    def get(variable, op, elems_expr):
        if op == 'in':
            return In(variable, elems_expr)
        elif op == 'notin':
            return Notin(variable, elems_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, variable, elems_expr):
        Predicate.__init__(self, variable)

        matches = re.match('\[(.*)\]', elems_expr)
        if not matches:
            raise attrs.InvalidExpression(elems_expr)

        elem_exprs = matches.group(1).split()

        self.rhs = map(self.variable.rhs_map, elem_exprs)


#################################
## Unary (boolean) expressions ##
#################################

class Assert(UnaryExpr):
    def _eval(self, boolexpr):
        return boolexpr

class Negate(UnaryExpr):
    def _eval(self, boolexpr):
        return not boolexpr

#####################################
## Binary (comparison) expressions ##
#####################################

class Eq(BinaryExpr):
    def __init__(self, variable, rhs_expr, is_re = False):
        BinaryExpr.__init__(self, variable, rhs_expr, is_re = is_re)

        if type(self.rhs) is re._pattern_type:
            self._call = lambda lhs: self.rhs.match(lhs) is not None
        else:
            self._call = lambda lhs: lhs == self.rhs

    def _eval(self, lhs):
        return self._call(lhs)

class Neq(BinaryExpr):
    def __init__(self, variable, rhs_expr, is_re = False):
        BinaryExpr.__init__(self, variable, rhs_expr, is_re = is_re)

        if type(self.rhs) is re._pattern_type:
            self._call = lambda lhs: self.rhs.match(lhs) is None
        else:
            self._call = lambda lhs: lhs != self.rhs

    def _eval(self, lhs):
        return self._call(lhs)

class Lt(BinaryExpr):
    def _eval(self, lhs):
        return lhs < self.rhs

class Gt(BinaryExpr):
    def _eval(self, lhs):
        return lhs > self.rhs

#########################################
## Set-element (inclusion) expressions ##
#########################################

class In(SetElementExpr):
    def _eval(self, lhs):
        if self.variable.vtype == attrs.Attr.NUMERIC_TYPE:
            return lhs in self.rhs
        else:
            for elem in self.rhs:
                if type(elem) is re._pattern_type:
                    if elem.match(lhs):
                        return True
                else:
                    if elem == lhs:
                        return True

            return False

class Notin(SetElementExpr):
    def _eval(self, lhs):
        if self.variable.vtype == attrs.Attr.NUMERIC_TYPE:
            return lhs not in self.rhs
        else:
            for elem in self.rhs:
                if type(elem) is re._pattern_type:
                    if elem.match(lhs):
                        return False
                else:
                    if elem == lhs:
                        return False

            return True

