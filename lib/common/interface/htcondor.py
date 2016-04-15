import htcondor

class HTCondor(object):
    """
    HTCondor interface.
    """

    def __init__(self, collector, schedd_constraint = ''):
        """
        Arguments:
          collector = host:port of the Collector instance.
        """

        self._collector = htcondor.Collector(collector)

        schedd_ads = self._collector.query(htcondor.AdTypes.Schedd, 'CMSGWMS_Type =?= "crabschedd"', ['ScheddIpAddr'])
        self._schedds = [htcondor.Schedd(ad) for ad in schedd_ads]

    def find_jobs(self, dataset = '', status = 0):
        """
        Return ClassAds for jobs matching the constraints.
        """

        constraint = '(%s)' % config.globalqueue.job_constraint

        if dataset:
            constraint += ' && DESIRED_CMSDataset == "%s"' % dataset

        if status != 0:
            constraint += ' && JobStatus =?= %d' % status

        classads = []

        for schedd in self._schedds:
            classads += schedd.xquery(constraint, ['DESIRED_CMSDataset','DAG_NodesTotal','DAG_NodesDone','DAG_NodesFailed','DAG_NodesQueued'])

        return classads
