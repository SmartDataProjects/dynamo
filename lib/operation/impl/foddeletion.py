from dynamo.operation.deletion import DeletionInterface
from dynamo.dataformat import DatasetReplica
from dynamo.fileop.rlfsm import RLFSM

class FODDeletionInterface(DeletionInterface):
    """
    DeletionInterface using the Dynamo FOD.
    """

    def __init__(self, config = None):
        DeletionInterface.__init__(self, config)
        self.rlfsm = RLFSM(config.fod)

    def schedule_deletion(self, replica, comments = ''): #override
        """
        Make file desubscriptions in FOD.
        Note: FOD does not have a concept of operation id.
        """

        if type(replica) is DatasetReplica:
            for block_replica in replica.block_replicas:
                self.rlfsm.desubscribe_files(block_replica)

            return {0: (True, replica.site, [replica.dataset])}
        else:
            self.rlfsm.desubscribe_files(replica)

            return {0: (True, replica.site, [replica.block])}

    def schedule_deletions(self, replica_list, comments = ''): #override
        items_by_site = {}
        for replica in replica_list:
            if replica.site not in items_by_site:
                items_by_site[replica.site] = []

            if type(replica) is DatasetReplica:
                for block_replica in replica.block_replicas:
                    self.rlfsm.desubscribe_files(block_replica)

                items_by_site[replica.site].append(replica.dataset)
            else:
                self.rlfsm.desubscribe_files(replica)

                items_by_site[replica.site].append(replica.block)

        return dict((0, (True, site, items)) for site, items in items_by_site.iteritems())
