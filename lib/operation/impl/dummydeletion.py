from dynamo.operation.deletion import DeletionInterface
from dynamo.dataformat import DatasetReplica

class DummyDeletionInterface(DeletionInterface):
    """
    DeletionInterface that actually does nothing.
    """

    def __init__(self, config = None):
        DeletionInterface.__init__(self, config)

    def schedule_deletion(self, replica, comments = ''): #override
        if type(replica) is DatasetReplica:
            return {0: (True, replica.site, [replica.dataset])}
        else:
            return {0: (True, replica.site, [replica.block])}

    def schedule_copies(self, replica_list, comments = ''): #override
        items_by_site = {}
        for replica in replica_list:
            if replica.site not in items_by_site:
                items_by_site[replica.site] = []

            if type(replica) is DatasetReplica:
                items_by_site[replica.site].append(replica.dataset)
            else:
                items_by_site[replica.site].append(replica.block)

        return dict((0, (True, site, items)) for site, items in items_by_site.iteritems())
