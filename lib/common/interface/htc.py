import logging
import re
import socket
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

        logger.info('Finding schedds reporting to collector %s', collector)

        schedd_ads = self._collector.query(htcondor.AdTypes.Schedd, schedd_constraint, ['ScheddIpAddr'])

        self._schedds = [htcondor.Schedd(ad) for ad in schedd_ads]

        for schedd, ad in zip(self._schedds, schedd_ads):
            matches = re.match('<([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)', ad['ScheddIpAddr'])
            # schedd does not have an ipaddr attribute natively, but we can assign it
            schedd.ipaddr = matches.group(1)
            schedd.host = socket.getnameinfo((matches.group(1), int(matches.group(2))), socket.AF_INET)[0] # socket.getnameinfo(*, AF_INET) returns a (host, port) 2-tuple

        logger.info('Found schedds: %s', ', '.join(['%s (%s)' % (schedd.host, schedd.ipaddr) for schedd in self._schedds]))

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
