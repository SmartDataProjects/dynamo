import logging

from dynamo.operation.copy import CopyInterface
from dynamo.dataformat import DatasetReplica

LOG = logging.getLogger(__name__)

class DummyCopyInterface(CopyInterface):
    """
    CopyInterface that actually does nothing.
    """

    def __init__(self, config = None):
        CopyInterface.__init__(self, config)

    def schedule_copy(self, replica, comments = ''): #override
        LOG.info('Ignoring copy schedule of %s', str(replica))

        if type(replica) is DatasetReplica:
            return {0: (True, replica.site, [replica.dataset])}
        else:
            return {0: (True, replica.site, [replica.block])}

    def schedule_copies(self, replica_list, comments = ''): #override
        LOG.info('Ignoring copy schedule of %d replicas', len(replica_list))

        items_by_site = {}
        for replica in replica_list:
            if replica.site not in items_by_site:
                items_by_site[replica.site] = []

            if type(replica) is DatasetReplica:
                items_by_site[replica.site].append(replica.dataset)
            else:
                items_by_site[replica.site].append(replica.block)

        return dict((0, (True, site, items)) for site, items in items_by_site.iteritems())
