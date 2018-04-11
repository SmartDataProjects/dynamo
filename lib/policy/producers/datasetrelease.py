import re
import collections
import logging

from dynamo.utils.interface.dbs import DBS

LOG = logging.getLogger(__name__)

class DatasetRelease(object):
    """
    Sets one attr:
      latest_production_release
    """

    produces = ['latest_production_release']

    def __init__(self, config):
        self._dbs = DBS(config.get('dbs', None))

    def load(self, inventory):
        latest_minor = collections.defaultdict(int)

        results = self._dbs.make_request('acquisitioneras')
        for result in results:
            release = result['acquisition_era_name']
            matches = re.match('CMSSW_([0-9]+)_([0-9]+)_([0-9]+)', release)
            if not matches:
                continue

            cycle = int(matches.group(1))
            major = int(matches.group(2))
            minor = int(matches.group(3))

            if minor > latest_minor[(cycle, major)]:
                latest_minor[(cycle, major)] = minor

        if LOG.getEffectiveLevel() == logging.DEBUG:
            LOG.debug('Latest releases:')
            for cm in sorted(latest_minor.keys()):
                LOG.debug(cm + (latest_minor[cm],))

        for dataset in inventory.datasets.itervalues():
            release = dataset.software_version
            if release is None:
                continue

            if release[2] == latest_minor[release[:2]]:
                dataset.attr['latest_production_release'] = True
