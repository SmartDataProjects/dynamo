import re
import fnmatch
import subprocess
from dataformat import DatasetReplica, BlockReplica

class InvalidExpression(Exception):
    pass

class Attr(object):
    """
    Base class representing an extended attribute of an object.
    Inherited classes should override get with object translation (e.g. replica -> dataset)
    and _get with the actual implementation of the attribute.
    """

    BOOL_TYPE, NUMERIC_TYPE, TEXT_TYPE, TIME_TYPE = range(4)

    def __init__(self, vtype, attr = '', args = None):
        self.vtype = vtype
        self.attr = attr
        self.args = args
        
    def get(self, obj):
        return self._get(obj)

    def _get(self, obj):
        if self.args is None:
            # simple attribute
            return getattr(obj, self.attr)
        else:
            # callable
            return getattr(obj, self.attr)(*self.args)

    def rhs_map(self, expr):
        """Map the rhs string in binary expressions. Raise if invalid."""

        if self.vtype == Attr.NUMERIC_TYPE:
            return float(expr)

        elif self.vtype == Attr.TEXT_TYPE:
            if '*' in expr or '?' in expr:
                return re.compile(fnmatch.translate(expr))
            else:
                return expr

        elif self.vtype == Attr.TIME_TYPE:
            proc = subprocess.Popen(['date', '-d', expr, '+%s'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            out, err = proc.communicate()
            if err != '':
                raise InvalidExpression('Invalid time expression %s' % expr)

            try:
                return float(out.strip())
            except:
                raise InvalidExpression('Invalid time expression %s' % expr)


class DatasetAttr(Attr):
    """Extract an attribute from the dataset regardless of the type of replica passed __call__"""

    def __init__(self, vtype, attr = None, args = None):
        Attr.__init__(self, vtype, attr = attr, args = args)

    def get(self, replica):
        if type(replica) is DatasetReplica:
            dataset = replica.dataset
        else:
            dataset = replica.block.dataset

        return self._get(dataset)


class DatasetReplicaAttr(Attr):
    """Extract an attribute from a dataset replica. If a block replica is passed, return the attribute of the owning dataset replica."""

    def __init__(self, vtype, attr = None, args = None):
        Attr.__init__(self, vtype, attr = attr, args = args)

    def get(self, replica):
        if type(replica) is BlockReplica:
            dataset_replica = replica.block.dataset.find_replica(replica.site)
            return self._get(dataset_replica)
        else:
            return self._get(replica)


class BlockReplicaAttr(Attr):
    """Extract an attribute from a block replica. If a dataset replica is passed, return a list of values."""

    def __init__(self, vtype, attr = None, args = None):
        Attr.__init__(self, vtype, attr = attr, args = args)

    def get(self, replica):
        if type(replica) is BlockReplica:
            return self._get(replica)
        else:
            return map(self._get, replica.block_replicas)


class ReplicaSiteAttr(Attr):
    """Extract an attribute from the site of a replica."""

    def __init__(self, vtype, attr = None, args = None):
        Attr.__init__(self, vtype, attr = attr, args = args)

    def get(self, replica):
        return self._get(replica.site)


class SiteAttr(Attr):
    """Extract an attribute from a site."""

    def __init__(self, vtype, attr = None, args = None):
        Attr.__init__(self, vtype, attr = attr, args = args)
        self.partition = None
