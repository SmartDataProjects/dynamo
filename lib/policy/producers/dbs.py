import re
import collections
import logging

from dynamo.utils.interface import RESTService
from dynamo.dataformat import Dataset

LOG = logging.getLogger(__name__)

class CheckAllDBS(object):
    """
    Check all given DBS instances and flag the datasets not known to all.
    Sets one attr:
      unknown_in_all_dbs
    """

    produces = ['unknown_in_all_dbs']

    def __init__(self, config):
        self._dbses = []
        for name, dbsconf in config.dbses.items():
            self._dbses.append(RESTService(dbsconf))

    def load(self, inventory):
        for dataset in inventory.datasets.itervalues():
            if dataset.status == Dataset.STAT_UNKNOWN:
                for dbs in self._dbses:
                    result = dbs.make_request('datasets', ['dataset=' + dataset.name, 'detail=true', 'dataset_access_type=*'])
                    if len(result) != 0:
                        break

                else:
                    dataset.attr['unknown_in_all_dbs'] = True
