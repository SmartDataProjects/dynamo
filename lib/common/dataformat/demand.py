class DatasetDemand(object):
    """Represents information on dataset demand."""

    def __init__(self, request_weight = -1., global_usage_rank = 0):
        self.request_weight = request_weight
        self.global_usage_rank = global_usage_rank
        self.local_usage_rank = {}
        self.locked_blocks = []

class DatasetRequest(object):
    """Represents a request to a dataset in the job queue"""

    def __init__(self, job_id, queue_time = 0, completion_time = 0, nodes_total = 0, nodes_done = 0, nodes_failed = 0, nodes_queued = 0):
        # queue_time & completion_time are unix timestamps in memory
        self.job_id = job_id
        self.queue_time = queue_time
        self.completion_time = completion_time
        self.nodes_total = nodes_total
        self.nodes_done = nodes_done
        self.nodes_failed = nodes_failed
        self.nodes_queued = nodes_queued

    def update(self, other):
        self.queue_time = other.queue_time
        self.completion_time = other.completion_time
        self.nodes_total = other.nodes_total
        self.nodes_done = other.nodes_done
        self.nodes_failed = other.nodes_failed
        self.nodes_queued = other.nodes_queued
