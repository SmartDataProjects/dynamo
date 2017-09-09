import re
import fnmatch
import subprocess

import detox.variables as variables
import detox.configuration as detox_config

class InvalidOperator(Exception):
    pass

class InvalidExpression(Exception):
    pass

##################
## Base classes ##
##################

class Predicate(object):
    @staticmethod
    def get(vardef, op = '', rhs_expr = ''):
        if op in UnaryExpr.operators:
            if rhs_expr != '':
                raise InvalidOperator(op)
            return UnaryExpr.get(vardef, op)

        elif op in BinaryExpr.operators:
            if rhs_expr == '':
                raise InvalidOperator(op)
            return BinaryExpr.get(vardef, op, rhs_expr)

        elif op in SetElementExpr.operators:
            if rhs_expr == '':
                raise InvalidOperator(op)
            return SetElementExpr.get(vardef, op, rhs_expr)

        else:
            raise InvalidOperator(op)

    def __init__(self, vmap, vtype):
        self.vmap = vmap
        self.vtype = vtype

    def __call__(self, obj):
        """
        Call _eval of the inherited classes.
        In case the LHS is a container (can happen when evaluating a block-level
        expression over a dataset replica), return the OR of _eval calls over the
        container elements.
        """

        lhs = self.vmap(obj)

        if not isinstance(lhs, basestring):
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
    def get(vardef, op):
        if op == '':
            return Assert(vardef)
        elif op == 'not':
            return Negate(vardef)

    def __init__(self, vardef):
        Predicate.__init__(self, *vardef)

        if self.vtype != variables.BOOL_TYPE:
            raise InvalidOperator(op)

class BinaryExpr(Predicate):
    operators = ['==', '!=', '<', '>', 'older_than', 'newer_than']

    @staticmethod
    def get(vardef, op, rhs_expr):
        if op == '==':
            return Eq(vardef, rhs_expr)
        elif op == '!=':
            return Neq(vardef, rhs_expr)
        elif op == '<' or op == 'older_than':
            return Lt(vardef, rhs_expr)
        elif op == '>' or op == 'newer_than':
            return Gt(vardef, rhs_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, vardef, rhs_expr):
        Predicate.__init__(self, *(vardef[:2]))

        if len(vardef) == 3:
            self.rhs = vardef[2](rhs_expr)
        elif self.vtype == variables.NUMERIC_TYPE:
            self.rhs = float(rhs_expr)
        elif self.vtype == variables.TEXT_TYPE:
            if '*' in rhs_expr or '?' in rhs_expr:
                self.rhs = re.compile(fnmatch.translate(rhs_expr))
            else:
                self.rhs = rhs_expr
        elif self.vtype == variables.TIME_TYPE:
            proc = subprocess.Popen(['date', '-d', rhs_expr, '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            out, err = proc.communicate()
            if err != '':
                raise InvalidExpression('Invalid time expression %s' % rhs_expr)

            try:
                self.rhs = float(out.strip()) + (detox_config.main.time_shift * 24. * 3600.)
            except:
                raise InvalidExpression('Invalid time expression %s' % rhs_expr)

class SetElementExpr(Predicate):
    operators = ['in', 'notin']

    @staticmethod
    def get(vardef, op, elems_expr):
        if op == 'in':
            return In(vardef, elems_expr)
        elif op == 'notin':
            return Notin(vardef, elems_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, vardef, elems_expr):
        Predicate.__init__(self, *vardef)

        matches = re.match('\[(.*)\]', elems_expr)
        if not matches:
            raise InvalidExpression(elems_expr)

        elems = matches.group(1).split()

        try:
            if self.vtype == variables.NUMERIC_TYPE:
                self.elems = map(int, elems)
            elif self.vtype == variables.TEXT_TYPE:
                self.elems = map(lambda s: re.compile(fnmatch.translate(s)), elems)
            else:
                raise Exception()
        except:
            raise InvalidExpression(matches.group(1))

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
    def __init__(self, vardef, rhs_expr):
        BinaryExpr.__init__(self, vardef, rhs_expr)

        if type(self.rhs) is re._pattern_type:
            self._call = lambda lhs: self.rhs.match(lhs) is not None
        else:
            self._call = lambda lhs: lhs == self.rhs

    def _eval(self, lhs):
        return self._call(lhs)

class Neq(BinaryExpr):
    def __init__(self, vardef, rhs_expr):
        BinaryExpr.__init__(self, vardef, rhs_expr)

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
    def _eval(self, elem):
        if self.vtype == variables.NUMERIC_TYPE:
            return elem in self.elems
        else:
            try:
                next(e for e in self.elems if e.match(elem))
                return True
            except StopIteration:
                return False

class Notin(SetElementExpr):
    def _eval(self, elem):
        return not In._eval(self, elem)
