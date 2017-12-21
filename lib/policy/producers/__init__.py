from crabaccess import CRABAccessHistory
from globalqueue import GlobalQueueRequestHistory
from mysqllock import MySQLReplicaLock
from weblock import WebReplicaLock

__all__ = [
    'CRABAccessHistory',
    'GlobalQueueRequestHistory',
    'MySQLReplicaLock',
    'WebReplicaLock'
]

producers = {}
for cls_name in __all__:
    for attr_name in eval(cls_name).produces:
        try:
            producers[attr_name].append(cls_name)
        except KeyError:
            producers[attr_name] = [cls_name]

__all__.append(producers)
