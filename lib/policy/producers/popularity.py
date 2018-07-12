import logging
import calendar

LOG = logging.getLogger(__name__)

try:
    from pop.engine import Engine
except ImportError:
    class Engine(object):
        def __init__(self):
            LOG.error('File popularity engine not found. Using a dummy class.')

        def get_namespace_usage_summary(self, namespace):
            return []

class FilePopularity(object):
    """
    Extracts the file usage.
    Sets one attr:
      last_access:  timestamp
      num_access:   int
    """

    produces = ['last_access', 'num_access']

    def __init__(self, config):
        self.pop_engine = Engine()
        # config.namespaces is a list of string pairs (namespace, replacement to map to LFN)
        # because config can only hold lists, convert them to tuples
        self.namespaces = map(tuple, config.namespaces)

    def load(self, inventory):

        # need namespace
        for namespace, replacement in self.namespaces:

            usage_summary = self.pop_engine.get_namespace_usage_summary(namespace)
    
            for (name,n_access,last_access) in usage_summary:
                
                # last_access is given in datetime.datetime
                utc_access = calendar.timegm(last_access.utctimetuple())
    
                lfn = replacement + name
                file_object = inventory.find_file(lfn)
                if file_object is None:
                    continue

                dataset = file_object.block.dataset
                attribute = dataset.attr
    
                if 'num_access' not in attribute:
                    attribute['num_access'] = float(n_access) / dataset.num_files
                else:
                    attribute['num_access'] += float(n_access) / dataset.num_files
    
                if 'last_access' not in attribute:
                    attribute['last_access'] = utc_access
                elif attribute['last_access'] < utc_access:
                    attribute['last_access'] = utc_access
