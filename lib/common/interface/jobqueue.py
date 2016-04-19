import collections

class JobQueueInterface(object):
    """
    Interface to job queue data.
    """

    def __init__(self):
        pass

    def get_dataset_requests(self, dataset = '', status = 0, start_time = 0, end_time = 0):
        """
        Return a list of DatasetRequests matching the constraints.
        """

        pass
