class CopiedReplica(object):
    __slots__ = ['dataset_name', 'size', 'status']

    def __init__(self, dataset_name, size, status):
        self.dataset_name = dataset_name
        self.size = size
        self.status = status

class DeletedReplica(object):
    __slots__ = ['dataset_name', 'size']
    
    def __init__(self, dataset_name, size):
        self.dataset_name = dataset_name
        self.size = size


class HistoryRecord(object):
    """Represents a transaction history record."""

    # operation types
    OP_COPY, OP_DELETE = range(2)
    # operation status
    ST_ENROUTE, ST_COMPLETE, ST_CANCELLED = range(1, 4)

    def __init__(self, operation_type, operation_id, site_name, timestamp = 0, approved = False, last_update = 0):
        self.operation_type = operation_type
        self.operation_id = operation_id
        self.site_name = site_name
        self.timestamp = timestamp
        self.approved = bool(approved)
        self.replicas = []

    def __str__(self):
        if self.operation_type == HistoryRecord.OP_COPY:
            op = 'COPY'
        else:
            op = 'DELETE'

        return 'HistoryRecord (%s, id=%d, site=%s, timestamp=%d, approved=%s)' % \
            (op, self.operation_id, self.site_name, self.timestamp, self.approved)

    def __repr__(self):
        if self.operation_type == HistoryRecord.OP_COPY:
            op = 'OP_COPY'
        else:
            op = 'OP_DELETE'

        return 'HistoryRecord(%s, %d, %s)' % (op, self.operation_id, self.site_name)
