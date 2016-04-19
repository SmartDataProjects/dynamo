import logging
import re
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

        for schedd, ad in zip(self._schedds, schedd_ads):
            matches = re.match('<([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):[0-9]+', ad['ScheddIpAddr'])
            # schedd does not have an ipaddr attribute natively, but we can assign it
            schedd.ipaddr = matches.group(1)

    def find_jobs(self, constraint = 'True', attributes = []):
        """
        Return ClassAds for jobs matching the constraints.
        """

        logger.info('Querying HTCondor with constraint "%s" for attributes %s', constraint, str(attributes))

        classads = []

        for schedd in self._schedds:
            attempt = 0
            while True:
                try:
                    ads = schedd.query(constraint, attributes)
                    break
                except IOError:
                    attempt += 1
                    logger.info('IOError in communicating with schedd %s. Trying again.', schedd.ipaddr)
                    if attempt == 10:
                        raise
                
            classads += ads

        logger.info('HTCondor returned %d classads', len(classads))

        return classads
