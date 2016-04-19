import logging
import htcondor

logger = logging.getLogger(__name__)

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

        schedd_ads = self._collector.query(htcondor.AdTypes.Schedd, schedd_constraint, ['ScheddIpAddr'])
        self._schedds = [htcondor.Schedd(ad) for ad in schedd_ads]

    def find_jobs(self, constraint = 'True', attributes = []):
        """
        Return ClassAds for jobs matching the constraints.
        """

        logger.info('Querying HTCondor with constraint "%s" for attributes %s', constraint, str(attributes))

        classads = []

        for schedd in self._schedds:
            classads += schedd.query(constraint, attributes)

        logger.info('HTCondor returned %d classads', len(classads))

        return classads
