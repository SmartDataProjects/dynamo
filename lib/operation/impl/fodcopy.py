import logging

from dynamo.operation.copy import CopyInterface
from dynamo.dataformat import DatasetReplica
from dynamo.fileop.rlfsm import RLFSM

LOG = logging.getLogger(__name__)

class FODCopyInterface(CopyInterface):
    """
    CopyInterface using the Dynamo FOD.
    """

    def __init__(self, config = None):
        CopyInterface.__init__(self, config)
        self.rlfsm = RLFSM(config.get('fod', None))

    def schedule_copy(self, replica, comments = ''): #override
        """
        Make file subscriptions in FOD.
        Note: FOD does not have a concept of operation id.
        """

        LOG.info('Scheduling copy of %s using RLFSM', str(replica))

        if type(replica) is DatasetReplica:
            for block_replica in replica.block_replicas:
                self.rlfsm.subscribe_files(block_replica)

            return {0: (True, replica.site, [replica.dataset])}
        else:
            self.rlfsm.subscribe_files(replica)

            return {0: (True, replica.site, [replica.block])}

    def schedule_copies(self, replica_list, comments = ''): #override
        LOG.info('Scheduling copy of %d replicas using RLFSM', len(replica_list))

        items_by_site = {}
        for replica in replica_list:
            if replica.site not in items_by_site:
                items_by_site[replica.site] = []

            if type(replica) is DatasetReplica:
                for block_replica in replica.block_replicas:
                    LOG.debug('Subscribing files for %s', str(block_replica))
                    self.rlfsm.subscribe_files(block_replica)

                items_by_site[replica.site].append(replica.dataset)
            else:
                LOG.debug('Subscribing files for %s', str(replica))
                self.rlfsm.subscribe_files(replica)

                items_by_site[replica.site].append(replica.block)

        return dict((0, (True, site, items)) for site, items in items_by_site.iteritems())
