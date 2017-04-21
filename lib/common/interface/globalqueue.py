import logging
import time

from common.interface.jobqueue import JobQueue
from common.interface.htc import HTCondor
import common.configuration as config

logger = logging.getLogger(__name__)

class GlobalQueue(JobQueue):
    """
    Interface to the CMS Global Queue.
    """

    def __init__(self, collector = config.globalqueue.collector):
        super(self.__class__, self).__init__()

        self.htcondor = HTCondor(collector, schedd_constraint = 'CMSGWMS_Type =?= "crabschedd"')

    def update(self, inventory): #override
        records = inventory.store.load_dataset_requests(inventory.datasets.values())
        full_request_list = records[1]

        constraint = 'TaskType=?="ROOT" && !isUndefined(DESIRED_CMSDataset) && (QDate > {last_update} || CompletionDate > {last_update})'.format(last_update = self._last_update)

        attributes = ['DESIRED_CMSDataset', 'GlobalJobId', 'QDate', 'CompletionDate', 'DAG_NodesTotal', 'DAG_NodesDone', 'DAG_NodesFailed', 'DAG_NodesQueued']
        
        job_ads = self.htcondor.find_jobs(constraint = constraint, attributes = attributes)

        job_ads.sort(key = lambda a: a['DESIRED_CMSDataset'])

        request_list = {}

        _dataset_name = ''
        dataset = None

        for ad in job_ads:
            if ad['DESIRED_CMSDataset'] != _dataset_name:
                _dataset_name = ad['DESIRED_CMSDataset']

                try:
                    dataset = inventory.datasets[_dataset_name]

                    if dataset not in full_request_list:
                        full_request_list[dataset] = {}

                    request_list[dataset] = {}

                except KeyError:
                    dataset = None

            if dataset is None:
                continue

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

            reqdata = (
                ad['QDate'],
                ad['CompletionDate'],
                nodes_total,
                nodes_done,
                nodes_failed,
                nodes_queued
            )

            job_id = ad['GlobalJobId']

            full_request_list[dataset][job_id] = reqdata
            request_list[dataset][job_id] = reqdata

        inventory.store.save_dataset_requests(request_list)

        self._last_update = time.time()

        self._compute(full_request_list)


def form_job_constraint(dataset, status, start_time, end_time):

    constraint = '(TaskType=?="ROOT" && !isUndefined(DESIRED_CMSDataset))'

    if dataset:
        constraint += ' && DESIRED_CMSDataset == "%s"' % dataset

    if status != 0:
        constraint += ' && JobStatus =?= %d' % status

    if start_time != 0:
        constraint += ' && QDate >= %d' % start_time

    if end_time != 0:
        constraint += ' && QDate <= %d' % end_time

    return constraint


if __name__ == '__main__':
    from argparse import ArgumentParser
    import collections
    import pprint

    parser = ArgumentParser(description = 'GlobalQueue interface')

    parser.add_argument('--collector', '-c', dest = 'collector', metavar = 'HOST:PORT', default = config.globalqueue.collector, help = 'Collector host.')
    parser.add_argument('--start-time', '-s', dest = 'start_time', metavar = 'TIME', type = int, default = 0, help = 'UNIX timestamp of beginning of the query range.')
    parser.add_argument('--end-time', '-e', dest = 'end_time', metavar = 'TIME', type = int, default = 0, help = 'UNIX timestamp of end of the query range.')
    parser.add_argument('--dataset', '-d', dest = 'dataset', metavar = 'NAME', default = '', help = 'Dataset name.')
    parser.add_argument('--status', '-t', dest = 'status', metavar = 'STATUS', type = int, default = 0, help = 'Job status.')
    parser.add_argument('--attributes', '-a', dest = 'attributes', metavar = 'ATTR', nargs = '+', default = None, help = 'Triggers "raw" output with specified attributes.')

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)

    from common.inventory import InventoryManager
    
    interface = GlobalQueue(args.collector)

    ads = interface.htcondor.find_jobs(constraint = form_job_constraint(args.dataset, args.status, args.start_time, args.end_time), attributes = args.attributes)

    print '['
    for ad in ads:
        print ' {'
        for key in sorted(ad.keys()):
            print '  "%s": %s,' % (key, str(ad[key]))
        if ad == ads[-1]:
            print ' }'
        else:
            print ' },'
    print ']'
