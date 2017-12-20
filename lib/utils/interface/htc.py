import sys
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
        @param collector         host:port of the Collector instance.
        @param schedd_constraint classad expression to narrow down the schedd selection.
        """

, schedd_constraint = 'CMSGWMS_Type =?= "crabschedd"')

        self._collector = htcondor.Collector(collector)

        logger.debug('Finding schedds reporting to collector %s', collector)

        attempt = 0
        while True:
            try:
                schedd_ads = self._collector.query(htcondor.AdTypes.Schedd, schedd_constraint, ['MyAddress'])
                break
            except IOError:
                attempt += 1
                logger.warning('Collector query failed: %s', str(sys.exc_info()[0]))
                if attempt == 10:
                    logger.error('Communication with the collector failed. We have no information of the condor pool.')
                    self._schedds = []
                    return

        self._schedds = []

        for ad in schedd_ads:
            schedd = htcondor.Schedd(ad)
            matches = re.match('<([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)', ad['MyAddress'])
            # schedd does not have an ipaddr attribute natively, but we can assign it
            schedd.ipaddr = matches.group(1)
            schedd.host = socket.getnameinfo((matches.group(1), int(matches.group(2))), socket.AF_INET)[0] # socket.getnameinfo(*, AF_INET) returns a (host, port) 2-tuple

            self._schedds.append(schedd)

        logger.debug('Found schedds: %s', ', '.join(['%s (%s)' % (schedd.host, schedd.ipaddr) for schedd in self._schedds]))

    def find_jobs(self, constraint = 'True', attributes = []):
        """
        Return ClassAds for jobs matching the constraints.
        """

        logger.debug('Querying HTCondor with constraint "%s" for attributes %s', constraint, str(attributes))

        classads = []

        for schedd in self._schedds:
            attempt = 0
            while True:
                try:
                    ads = schedd.query(constraint, attributes)
                    break
                except IOError:
                    attempt += 1
                    logger.warning('IOError in communicating with schedd %s. Trying again.', schedd.ipaddr)
                    if attempt == 10:
                        logger.error('Schedd %s did not respond.', schedd.ipaddr)
                        ads = []
                        break
                
            classads.extend(ads)

        logger.info('HTCondor returned %d classads', len(classads))

        return classads


if __name__ == '__main__':
    
    import sys
    import pprint
    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'HTCondor interface')

    parser.add_argument('--log-level', '-l', metavar = 'LEVEL', dest = 'log_level', default = '', help = 'Logging level.')
    parser.add_argument('--pool', '-p', dest = 'pool', metavar = 'COLLECTOR', default = None, help = 'Condor pool to query.')
    parser.add_argument('--schedd-const', '-s', dest = 'schedd_const', metavar = 'EXPR', default = '', help = 'Schedd constraint.')
    parser.add_argument('--job-const', '-c', dest = 'job_const', metavar = 'EXPR', default = 'True', help = 'Job constraint.')
    parser.add_argument('--attributes', '-a', dest = 'attributes', metavar = 'ATT', nargs = '+', default = [], help = 'Attributes to extract.')

    args = parser.parse_args()
    sys.argv = []

    if args.log_level:
        try:
            level = getattr(logging, args.log_level.upper())
            logging.getLogger().setLevel(level)
        except AttributeError:
            logging.warning('Log level ' + args.log_level + ' not defined')

    interface = HTCondor(args.pool, args.schedd_const)
    
    pprint.pprint(interface.find_jobs(args.job_const, args.attributes))
