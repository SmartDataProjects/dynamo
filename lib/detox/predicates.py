import re
import fnmatch
import subprocess

import detox.variables as variables

class InvalidOperator(Exception):
    pass

class InvalidExpression(Exception):
    pass

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
        elif op in SetExpr.operators:
            if rhs_expr == '':
                raise InvalidOperator(op)

            return SetExpr.get(vardef, op, rhs_expr)
        else:
            raise InvalidOperator(op)

    def __init__(self, vmap, vtype):
        self.vmap = vmap
        self.vtype = vtype

    def __call__(self, obj):
        return self.vmap(obj)

class UnaryExpr(Predicate):
    operators = ['', 'not']

    @staticmethod
    def get(vardef, op):
        if op == '':
            return Predicate(*vardef)
        elif op == 'not':
            return Negate(*vardef)

    def __init__(self, vardef):
        Predicate.__init__(self, *vardef)

        if self.vtype != variables.BOOL_TYPE:
            raise InvalidOperator(op)

class Negate(UnaryExpr):
    def __call__(self, obj):
        return not self.vmap(obj)

class BinaryExpr(Predicate):
    operators = ['==', '!=', '<', '>']

    @staticmethod
    def get(vardef, op, rhs_expr):
        if op == '==':
            return Eq(vardef, rhs_expr)
        elif op == '!=':
            return Neq(vardef, rhs_expr)
        elif op == '<':
            return Lt(vardef, rhs_expr)
        elif op == '>':
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
            if '*' in rhs_expr:
                self.rhs = re.compile(fnmatch.translate(rhs_expr))
            else:
                self.rhs = rhs_expr
        elif self.vtype == variables.TIME_TYPE:
            proc = subprocess.Popen(['date', '-d', rhs_expr, '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            out, err = proc.communicate()
            if err != '':
                raise InvalidExpression('Invalid time expression %s' % rhs_expr)

            try:
                self.rhs = float(out.strip())
            except:
                raise InvalidExpression('Invalid time expression %s' % rhs_expr)

class Eq(BinaryExpr):
    def __init__(self, vardef, rhs_expr):
        BinaryExpr.__init__(self, vardef, rhs_expr)

        if type(self.rhs) is re._pattern_type:
            self._call = lambda obj: self.rhs.match(self.vmap(obj)) is not None
        else:
            self._call = lambda obj: self.vmap(obj) == self.rhs

    def __call__(self, obj):
        return self._call(obj)

class Neq(BinaryExpr):
    def __init__(self, vardef, rhs_expr):
        BinaryExpr.__init__(self, vardef, rhs_expr)

        if type(self.rhs) is re._pattern_type:
            self._call = lambda obj: self.rhs.match(self.vmap(obj)) is None
        else:
            self._call = lambda obj: self.vmap(obj) != self.rhs

    def __call__(self, obj):
        return self._call(obj)

class Lt(BinaryExpr):
    def __call__(self, obj):
        return self.vmap(obj) < self.rhs

class Gt(BinaryExpr):
    def __call__(self, obj):
        return self.vmap(obj) > self.rhs

class PatternExpr(BinaryExpr):
    def __init__(self, vardef, pattern):
        Predicate.__init__(self, *vardef)

        if '*' in pattern:
            self.rhs = re.compile(fnmatch.translate(pattern))
            self.is_re = True
        else:
            self.rhs = pattern
            self.is_re = False

class Match(PatternExpr):
    def __call__(self, obj):
        if self.is_re:
            return self.rhs.match(self.vmap(obj)) is not None
        else:
            return self.vmap(obj) == self.rhs

class Unmatch(PatternExpr):
    def __call__(self, obj):
        if self.is_re:
            return self.rhs.match(self.vmap(obj)) is None
        else:
            return self.vmap(obj) != self.rhs

class SetExpr(Predicate):
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

class In(SetExpr):
    def __call__(self, obj):
        if self.vtype == variables.NUMERIC_TYPE:
            return self.vmap(obj) in self.elems
        else:
            v = self.vmap(obj)
            try:
                next(e for e in self.elems if e.match(v))
                return True
            except StopIteration:
                return False

class Notin(SetExpr):
    def __call__(self, obj):
        return not In.__call__(self, obj)
