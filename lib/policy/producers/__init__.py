"""
Classes in this package produce dataset attributes (Dataset.attr).
The classes must provide three methods with the following signature:
  __init__(self, config)
  load(self, inventory)
"""

from crabaccess import CRABAccessHistory
from globalqueue import GlobalQueueRequestHistory
from mysqllock import MySQLReplicaLock
from weblock import WebReplicaLock
from protectedsite import ProtectedSiteTagger
from datasetrelease import DatasetRelease

__all__ = [
    'CRABAccessHistory',
    'GlobalQueueRequestHistory',
    'MySQLReplicaLock',
    'WebReplicaLock',
    'ProtectedSiteTagger',
    'DatasetRelease'
]

# Dictionary of registered producers
# product attribute name -> list of producers
producers = {}
for cls_name in __all__:
    for attr_name in eval(cls_name).produces:
        try:
            producers[attr_name].append(cls_name)
        except KeyError:
            producers[attr_name] = [cls_name]

# Export the producers dictionary
__all__.append(producers)
