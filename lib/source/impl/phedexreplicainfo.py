"""
ReplicaInfoSource using PhEDEx.
"""

import logging

from source.replicainfo import ReplicaInfoSource
from common.interface.phedex import PhEDEx
from dataformat import Dataset, Block, DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class PhEDExReplicaInfoSource(ReplicaInfoSource):
    def __init__(self, config):
        ReplicaInfoSource.__init__(self, config)

        self._phedex = PhEDEx()

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

    def get_updated_replicas(self, updated_since): #override
        updated_replicas = []

        result = self._phedex.make_request('blockreplicas', ['show_dataset=y', 'update_since=%d' % updated_since])

        for dataset_entry in result:
            dataset = Dataset(
                dataset_entry['name'],
                size = dataset_entry['bytes'],
                num_files = dataset_entry['files'],
                is_open = (dataset_entry['is_open'] == 'y')
            )
            
            for block_entry in dataset_entry['block']:
                name = block_entry['name']
                block_name = Block.translate_name(name[name.find('#') + 1:])

                block = Block(
                    block_name,
                    dataset,
                    size = block_entry['bytes'],
                    num_files = block_entry['files'],
                    is_open = (block_entry['is_open'] == 'y')
                )

                for replica_entry in block_entry['replica']:
                    if replica_entry['group'] is None:
                        group = None
                    else:
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

                    updated_replicas.append(block_replica)

        return updated_replicas

    def get_deleted_replicas(self, deleted_since): #override
        deleted_replicas = []

        result = self._phedex.make_request('deletions', ['complete_since=%d' % deleted_since])

        for dataset_entry in result:
            dataset = Dataset(dataset_entry['name'])
            
            for block_entry in dataset_entry['block']:
                name = block_entry['name']
                block_name = Block.translate_name(name[name.find('#') + 1:])

                block = Block(block_name, dataset)

                for deletion_entry in block_entry['deletion']:
                    block_replica = BlockReplica(block, Site(replica_entry['node']))

                    deleted_replicas.append(block_replica)

        return deleted_replicas
