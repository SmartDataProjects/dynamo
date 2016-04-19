import logging
import collections

from common.interface.jobqueue import JobQueueInterface
from common.interface.htc import HTCondor
from common.dataformat import DatasetRequest
import common.configuration as config

logger = logging.getLogger(__name__)

class GlobalQueue(JobQueueInterface):
    """
    Interface to CMS Global Queue.
    """

    def __init__(self, collector = config.globalqueue.collector):
        self.htcondor = HTCondor(collector, schedd_constraint = 'CMSGWMS_Type =?= "crabschedd"')

    def get_dataset_requests(self, dataset = '', status = 0, start_time = 0, end_time = 0): #override
        constraint = '(TaskType=?="ROOT" && !isUndefined(DESIRED_CMSDataset))'

        if dataset:
            constraint += ' && DESIRED_CMSDataset == "%s"' % dataset

        if status != 0:
            constraint += ' && JobStatus =?= %d' % status

        if start_time != 0:
            constraint += ' && CompletionDate >= %d' % start_time

        if end_time != 0:
            constraint += ' && CompletionDate <= %d' % end_time

        attributes = ['DESIRED_CMSDataset', 'GlobalJobId', 'QDate', 'CompletionDate', 'DAG_NodesTotal', 'DAG_NodesDone', 'DAG_NodesFailed','DAG_NodesQueued']
        
        job_ads = self.htcondor.find_jobs(constraint = constraint, attributes = attributes)

        requests = []
        for ad in job_ads:
            try:
                nodes_total = ad['DAG_NodesTotal']
                nodes_done = ad['DAG_NodesDone']
                nodes_failed = ad['DAG_NodesFailed']
                nodes_queued = ad['DAG_NodesQueued']
            except KeyError:
                nodes_total = 0
                nodes_done = 0
                nodes_failed = 0
                nodes_queued = 0

            requests.append((
                ad['DESIRED_CMSDataset'],
                DatasetRequest(
                    job_id = ad['GlobalJobId'],
                    queue_time = ad['QDate'],
                    completion_time = ad['CompletionDate'],
                    nodes_total = nodes_total,
                    nodes_done = nodes_done,
                    nodes_failed = nodes_failed,
                    nodes_queued = nodes_queued
                )
            ))

        return requests


if __name__ == '__main__':
    from argparse import ArgumentParser
    import pprint

    parser = ArgumentParser(description = 'GlobalQueue interface')

    parser.add_argument('--collector', '-c', dest = 'collector', metavar = 'HOST:PORT', default = config.globalqueue.collector, help = 'Collector host.')
    parser.add_argument('--start-time', '-s', dest = 'start_time', metavar = 'TIME', default = 0, help = 'UNIX timestamp of beginning of the query range.')
    parser.add_argument('--end-time', '-e', dest = 'end_time', metavar = 'TIME', default = 0, help = 'UNIX timestamp of end of the query range.')
    parser.add_argument('--dataset', '-d', dest = 'dataset', metavar = 'NAME', default = '', help = 'Dataset name.')
    parser.add_argument('--status', '-t', dest = 'status', metavar = 'STATUS', type = int, default = 0, help = 'Job status.')

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    
    interface = GlobalQueue(args.collector)

    requests = interface.get_dataset_requests(dataset = args.dataset, status = args.status, start_time = args.start_time, end_time = args.end_time)

    class Counter(object):
        def __init__(self, num_requests = 0, num_run = 0, num_nodes = 0):
            self.num_requests = 0
            self.num_run = 0
            self.num_nodes = 0

        def __repr__(self):
            return 'Counter(num_requests = %d, num_run = %d, num_nodes = %d)' % (self.num_requests, self.num_run, self.num_nodes)

        def __str__(self):
            return ' #req = %d\n #run = %d\n #nodes = %d' % (self.num_requests, self.num_run, self.num_nodes)

    out_data = collections.defaultdict(Counter)
    for dataset_name, request in requests:
        counter = out_data[dataset_name]
        counter.num_requests += 1
        if request.nodes_total != 0:
            counter.num_run += 1
        counter.num_nodes += request.nodes_total

    for dataset, counter in out_data.items():
        print dataset + ':\n' + str(counter)
