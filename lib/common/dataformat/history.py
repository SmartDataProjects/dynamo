import collections

class HistoryRecord(object):
    """Represents a transaction history record."""

    # operation types
    OP_COPY, OP_DELETE = range(2)

    CopiedReplica = collections.namedtuple('CopiedReplica', ['dataset_name'])
    DeletedReplica = collections.namedtuple('DeletedReplica', ['dataset_name'])

    def __init__(self, operation_type, operation_id, site_name, timestamp = 0, approved = False, size = 0, completed = False, last_update = 0):
        self.operation_type = operation_type
        self.operation_id = operation_id
        self.site_name = site_name
        self.timestamp = timestamp
        self.approved = bool(approved)
        self.size = size
        self.completed = completed
        self.replicas = []

    def __del__(self):
        del self.replicas
