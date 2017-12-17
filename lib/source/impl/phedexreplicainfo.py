"""
ReplicaInfoSource using PhEDEx.
"""

import logging

from source.replicainfo import ReplicaInfoSource
from common.interface.phedex import PhEDEx
from dataformat import Group, Site, Dataset, Block, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class PhEDExReplicaInfoSource(ReplicaInfoSource):
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

        return len(source) != 0

    def get_replicas(self, site = None, dataset = None, block = None): #override
        options = []
        if site is not None:
            options.append('node=' + site)
        if dataset is not None:
            options.append('dataset=' + dataset)
        if block is not None:
            options.append('block=' + block)

        if len(options) == 0:
            return []
        
        result = self._phedex.make_request('blockreplicas', ['show_dataset=y'] + options)

        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_blockreplicas)

    def get_updated_replicas(self, updated_since): #override
        result = self._phedex.make_request('blockreplicas', ['show_dataset=y', 'update_since=%d' % updated_since])
        
        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_blockreplicas)

    def get_deleted_replicas(self, deleted_since): #override
        result = self._phedex.make_request('deletions', ['complete_since=%d' % deleted_since])

        return PhEDExReplicaInfoSource.make_block_replicas(result, PhEDExReplicaInfoSource.maker_deletions)

    @staticmethod
    def make_block_replicas(dataset_entries, replica_maker):
        """Return a list of block replicas linked to Dataset, Block, Site, and Group"""

        block_replicas = []

        for dataset_entry in dataset_entries:
            dataset = Dataset(
                dataset_entry['name'],
                size = dataset_entry['bytes'],
                num_files = dataset_entry['files'],
                is_open = (dataset_entry['is_open'] == 'y')
            )
            
            for block_entry in dataset_entry['block']:
                name = block_entry['name']
                block_name = Block.to_internal_name(name[name.find('#') + 1:])

                block = Block(
                    block_name,
                    dataset,
                    size = block_entry['bytes'],
                    num_files = block_entry['files']
                )
                try:
                    block.is_open = (block_entry['is_open'] == 'y')
                except KeyError:
                    pass

                block_replicas.extend(replica_maker(block, block_entry))

        return block_replicas

    @staticmethod
    def maker_blockreplicas(block, block_entry):
        replicas = []

        for replica_entry in block_entry['replica']:
            group = Group(replica_entry['group'])

            block_replica = BlockReplica(
                block,
                Site(replica_entry['node']),
                group,
                is_complete = (replica_entry['bytes'] == block.size),
                is_custodial = (replica_entry['custodial'] == 'y'),
                size = replica_entry['bytes'],
                last_update = int(replica_entry['time_update'])
            )

            replicas.append(block_replica)

        return replicas

    @staticmethod
    def maker_deletions(block, block_entry):
        replicas = []

        for deletion_entry in block_entry['deletion']:
            block_replica = BlockReplica(block, Site(deletion_entry['node']), Group(None))

            replicas.append(block_replica)

        return replicas
