"""
Classes in this package produce dataset attributes (Dataset.attr).
The classes must provide two methods with the following signature:
  __init__(self, config)
  load(self, inventory)
and define a list cls.produces to indicate the names of the attributes
the load() function adds to the datasets.
"""

from crabaccess import CRABAccessHistory
from globalqueue import GlobalQueueRequestHistory
from mysqllock import MySQLReplicaLock
from weblock import WebReplicaLock
from protectedsite import ProtectedSiteTagger
from enforcerprotected import EnforcedProtectionTagger
from datasetrelease import DatasetRelease
from relativeage import BlockReplicaRelativeAge
from dbs import CheckAllDBS

__all__ = [
    'CRABAccessHistory',
    'GlobalQueueRequestHistory',
    'MySQLReplicaLock',
    'WebReplicaLock',
    'ProtectedSiteTagger',
    'EnforcedProtectionTagger',
    'DatasetRelease',
    'BlockReplicaRelativeAge',
    'CheckAllDBS'
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
