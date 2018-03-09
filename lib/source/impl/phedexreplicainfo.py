import logging

from dynamo.source.replicainfo import ReplicaInfoSource
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.dataformat import Group, Site, Dataset, Block, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class PhEDExReplicaInfoSource(ReplicaInfoSource):
    """ReplicaInfoSource using PhEDEx."""

    def __init__(self, config):
        ReplicaInfoSource.__init__(self, config)

        self._phedex = PhEDEx(config.phedex)

    def replica_exists_at_site(self, site, item): #override
        options = ['node=' + site.name]

        if type(item) == Dataset:
            options += ['dataset=' + item.name, 'show_dataset=y']
        elif type(item) == DatasetReplica:
            options += ['dataset=' + item.dataset.name, 'show_dataset=y']
        elif type(item) == Block:
            options += ['block=' + item.full_name()]
        elif type(item) == BlockReplica:
            options += ['block=' + item.block.full_name()]
        else:
            raise RuntimeError('Invalid input passed: ' + repr(item))
        
        source = self._phedex.make_request('blockreplicas', options)

        if len(source) != 0:
            return True

        # blockreplicas has max ~20 minutes latency
        source = self._phedex.make_request('subscriptions', options)

        return len(source) != 0

    def get_replicas(self, site = None, dataset = None, block = None): #override
        options = []
        if site is not None:
            options.append('node=' + site)
        if dataset is not None:
            options.append('dataset=' + dataset)
        if block is not None:
            options.append('block=' + block)

        LOG.info('get_replicas(' + ','.join(options) + ')  Fetching the list of replicas from PhEDEx')

        if len(options) == 0:
            return []
        
        result = self._phedex.make_request('blockreplicas', ['show_dataset=y'] + options)

        block_replicas = PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_blockreplicas)
        
        # Also use subscriptions call which has a lower latency than blockreplicas
        # For example, group change on a block replica at time T may not show up in blockreplicas until up to T + 15 minutes
        # while in subscriptions it is visible within a few seconds
        # But subscriptions call without a dataset or block takes too long
        if dataset is None and block is None:
            return block_replicas

        result = self._phedex.make_request('subscriptions', options)

        for dataset_entry in result:
            dataset_name = dataset_entry['name']

            try:
                subscriptions = dataset_entry['subscription']
            except KeyError:
                pass
            else:
                for sub_entry in subscriptions:
                    site_name = sub_entry['node']
                    for replica in block_replicas:
                        if replica.block.dataset.name == dataset_name and replica.site.name == site_name:
                            replica.group = Group(sub_entry['group'])
                            replica.is_custodial = (sub_entry['custodial'] == 'y')

            try:
                block_entries = dataset_entry['block']
            except KeyError:
                pass
            else:
                for block_entry in block_entries:
                    _, block_name = Block.from_full_name(block_entry['name'])

                    try:
                        subscriptions = block_entry['subscription']
                    except KeyError:
                        pass
                    else:
                        for sub_entry in subscriptions:
                            site_name = sub_entry['node']
                            for replica in block_replicas:
                                if replica.block.dataset.name == dataset_name and \
                                        replica.block.name == block_name and \
                                        replica.site.name == site_name:

                                    replica.group = Group(sub_entry['group'])
                                    replica.is_complete = (sub_entry['node_bytes'] == block_entry['bytes'])
                                    replica.is_custodial = (sub_entry['custodial'] == 'y')
                                    replica.size = sub_entry['node_bytes']
                                    if sub_entry['time_update'] is not None:
                                        replica.last_update = 0
                                    else:
                                        replica.last_update = int(sub_entry['time_update'])

        return block_replicas

    def get_updated_replicas(self, updated_since): #override
        LOG.info('get_updated_replicas(%d)  Fetching the list of replicas from PhEDEx', updated_since)

        result = self._phedex.make_request('blockreplicas', ['show_dataset=y', 'update_since=%d' % updated_since])
        
        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_blockreplicas)

    def get_deleted_replicas(self, deleted_since): #override
        LOG.info('get_deleted_replicas(%d)  Fetching the list of replicas from PhEDEx', deleted_since)

        result = self._phedex.make_request('deletions', ['complete_since=%d' % deleted_since])

        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_deletions)

    @staticmethod
    def make_block_replicas(dataset_entries, replica_maker):
        """Return a list of block replicas linked to Dataset, Block, Site, and Group"""

        block_replicas = []

        for dataset_entry in dataset_entries:
            dataset = Dataset(
                dataset_entry['name']
            )
            
            for block_entry in dataset_entry['block']:
                try:
                    _, block_name = Block.from_full_name(block_entry['name'])
                except ValueError: # invalid name
                    continue

                block = Block(
                    block_name,
                    dataset,
                    block_entry['bytes']
                )

                block_replicas.extend(replica_maker(block, block_entry))

        return block_replicas

    @staticmethod
    def maker_blockreplicas(block, block_entry):
        replicas = []

        for replica_entry in block_entry['replica']:
            time_update = replica_entry['time_update']
            if time_update is None:
                time_update = 0

            block_replica = BlockReplica(
                block,
                Site(replica_entry['node']),
                Group(replica_entry['group']),
                is_complete = (replica_entry['bytes'] == block.size),
                is_custodial = (replica_entry['custodial'] == 'y'),
                size = replica_entry['bytes'],
                last_update = int(time_update)
            )

            replicas.append(block_replica)

        return replicas

    @staticmethod
    def maker_deletions(block, block_entry):
        replicas = []

        for deletion_entry in block_entry['deletion']:
            block_replica = BlockReplica(block, Site(deletion_entry['node']), Group.null_group)

            replicas.append(block_replica)

        return replicas
