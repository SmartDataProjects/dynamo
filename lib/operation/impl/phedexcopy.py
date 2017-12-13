"""Copy using PhEDEx."""

import logging
import collections

from operation.copy import CopyInterface
from common.interface.webservice import POST
from common.interface.phedex import PhEDEx
from dataformat import DatasetReplica, BlockReplica

LOG = logging.getLogger(__name__)

class PhEDExCopyInterface(CopyInterface):
    def __init__(self, config):
        CopyInterface.__init__(self, config)

        self._phedex = PhEDEx(config.phedex)

        self.subscription_chunk_size = config.chunk_size * 1.e+12

    def schedule_copy(self, replica,  comments = ''): #override
        request_mapping = {}

        subscription_list = []

        if type(replica) is DatasetReplica:
            blocks_by_group = collections.defaultdict(set)
            for block_replica in replica.block_replicas:
                blocks_by_group[block_replica.group].add(block_replica.block)

            if len(blocks_by_group) > 1:
                # this was called as a dataset-level copy, but in fact we have multiple
                # sets of blocks with different groups -> recall block-level schedule_copies
                return self.schedule_copies(replica.block_replicas, comments)

            group, block_replicas = blocks_by_group.items()[0]

            if block_replicas == replica.dataset.blocks:
                subscription_list.append(replica.dataset)
                level = 'dataset'
            else:
                subscription_list.extend(block_replicas)
                level = 'block'

        else: #BlockReplica
            group = replica.group
            subscription_list.append(replica.block)
            level = 'block'

        self._run_subscription_request(request_mapping, replica.site, group, level, subscription_list, comments)

        return request_mapping

    def schedule_copies(self, replicas, comments = ''): #override
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        for replica in replicas:
            replicas_by_site[replica.site].append(replica)

        for site, replica_list in replicas_by_site.iteritems():
            # sort the subscriptions into dataset level / block level and by groups
            subscription_lists = {}
            subscription_lists['dataset'] = collections.defaultdict(list) # {(level, group_name): [replicas]}
            subscription_lists['block'] = collections.defaultdict(list) # {(level, group_name): [replicas]}

            for replica in replica_list:
                if type(replica) is DatasetReplica:
                    blocks_by_group = collections.defaultdict(set)
                    for block_replica in replica.block_replicas:
                        blocks_by_group[block_replica.group].add(block_replica.block)

                    for group, blocks in blocks_by_group.iteritems():
                        if blocks == replica.dataset.blocks:
                            subscription_lists['dataset'][group].append(replica.dataset)
                        else:
                            subscription_lists['block'][group].extend(blocks)
                else:
                    subscription_lists['block'][group].append(replica.block)

            for level in ['dataset', 'block']:
                for group, items in subscription_lists[level].iteritems():
                    self._run_subscription_request(request_mapping, site, group, level, items, comments)

        return request_mapping

    def _run_subscription_request(self, request_mapping, site, group, level, subscription_list, comments):
        # Make a subscription request for potentitally multiple datasets or blocks but to one site and one group
        full_catalog = collections.defaultdict(list)

        if level == 'dataset':
            for dataset in subscription_list:
                full_catalog[dataset] = []
        elif level == 'block':
            for block in subscription_list:
                full_catalog[dataset].append(block)

        LOG.info('Subscribing %d datasets for %s at %s', len(full_catalog), group.name, site.name)

        # make requests in chunks
        request_catalog = {}
        chunk_size = 0
        items = []
        while len(full_catalog) != 0:
            dataset, blocks = full_catalog.popitem()
            request_catalog[dataset] = blocks

            if level == 'dataset':
                chunk_size += dataset.size
                items.append(dataset)
            elif level == 'block':
                chunk_size += sum(b.size for b in blocks)
                items.extend(blocks)
            
            if chunk_size < self.subscription_chunk_size and len(full_catalog) != 0:
                continue

            options = {
                'node': site.name,
                'data': self._phedex.form_catalog_xml(request_catalog),
                'level': level,
                'priority': 'normal',
                'move': 'n',
                'static': 'n',
                'custodial': 'n',
                'group': group.name,
                'request_only': 'n',
                'no_mail': 'n',
                'comments': comments
            }
    
            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
            try:
                result = self._phedex.make_request('subscribe', options, method = POST)
            except:
                result = []

            if len(result) != 0:
                request_id = int(result[0]['id']) # return value is a string
                LOG.warning('PhEDEx subscription request id: %d', request_id)
                request_mapping[request_id] = (True, site, items)
            else:
                LOG.error('Copy %s failed.', str(options))
                # we should probably do something here

            request_catalog = {}
            chunk_size = 0
            items = []

    def copy_status(self, request_id): #override
        request = self._phedex.make_request('transferrequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        site_name = request[0]['destinations']['node'][0]['name']

        dataset_names = []
        for ds_entry in request[0]['data']['dbs']['dataset']:
            dataset_names.append(ds_entry['name'])

        block_names = []
        for ds_entry in request[0]['data']['dbs']['block']:
            block_names.append(ds_entry['name'])

        subscriptions = []

        if len(dataset_names) != 0:
            chunks = [dataset_names[i:i + 35] for i in xrange(0, len(dataset_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._phedex.make_request('subscriptions', ['node=%s' % site_name] + ['dataset=%s' % n for n in chunk]))

        if len(block_names) != 0:
            chunks = [block_names[i:i + 35] for i in xrange(0, len(block_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._phedex.make_request('subscriptions', ['node=%s' % site_name] + ['block=%s' % n for n in chunk]))

        status = {}
        for dataset in subscriptions:
            try:
                cont = dataset['subscription'][0]
                bytes = dataset['bytes']
                node_bytes = cont['node_bytes']
                time_update = cont['time_update']
            except KeyError:
                # this was a block-level subscription (no 'subscription' field for the dataset)
                bytes = 0
                node_bytes = 0
                time_update = 0
                for block in dataset['block']:
                    cont = block['subscription'][0]
                    bytes += block['bytes']
                    node_bytes += cont['node_bytes']
                    time_update = max(time_update, cont['time_update'])

            status[(site_name, dataset['name'])] = (bytes, node_bytes, time_update)

        return status
