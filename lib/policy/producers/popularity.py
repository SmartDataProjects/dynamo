import logging

LOG = logging.getLogger(__name__)

try:
    from pop.engine import engine
except ImportError:
    LOG.error('File popularity engine not found. Using a dummy class.')

    class engine(object):
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
        self.pop_engine = engine()
        # config.namespaces is a list of string pairs (namespace, replacement to map to LFN)
        # because config can only hold lists, convert them to tuples
        self.namespaces = map(tuple, config.namespaces)

    def load(self, inventory):

        # need namespace
        for namespace, replacement in self.namespaces:

            usage_summary = self.pop_engine.get_namespace_usage_summary(namespace)
    
            for (name,n_accesses,last_access) in usage_summary:
    
                lfn = replacement + name
                file_object = inventory.find_file(lfn)
                dataset = file_object.block.dataset
                attribute = dataset.attr
    
                if 'num_access' not in attribute:
                    attribute['num_access'] = float(n_access) / len(dataset.num_files)
                else:
                    attribute['num_access'] += float(n_access) / len(dataset.num_files)
    
                if 'last_access' not in attribute:
                    attribute['last_access'] = access
                elif attribute['last_access'] < access:
                    attribute['last_access'] = access
