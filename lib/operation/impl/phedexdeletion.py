import logging
import collections

from dynamo.operation.deletion import DeletionInterface
from dynamo.utils.interface.webservice import POST
from dynamo.utils.interface.phedex import PhEDEx
from dynamo.dataformat import DatasetReplica, BlockReplica, Site, Configuration

LOG = logging.getLogger(__name__)

class PhEDExDeletionInterface(DeletionInterface):
    """Deletion using PhEDEx."""

    def __init__(self, config = None):
        config = Configuration(config)

        DeletionInterface.__init__(self, config)

        self._phedex = PhEDEx(config.get('phedex', None))

        self.auto_approval = config.get('auto_approval', True)
        self.allow_tape_deletion = config.get('allow_tape_deletion', False)
        self.tape_auto_approval = config.get('tape_auto_approval', False)

        self.deletion_chunk_size = config.get('chunk_size', 50.) * 1.e+12

        if self.dry_run:
            self._next_operation_id = 1

    def schedule_deletion(self, replica, comments = ''): #override
        request_mapping = {}

        if replica.site.storage_type == Site.TYPE_MSS and self.allow_tape_deletion:
            LOG.warning('Deletion from MSS is not allowed by configuration.')
            return request_mapping

        deletion_list = []
        if type(replica) is DatasetReplica:
            replica_blocks = set(r.block for r in replica.block_replicas)

            if replica_blocks == replica.dataset.blocks:
                deletion_list.append(replica.dataset)
                level = 'dataset'
            else:
                deletion_list.extend(replica_blocks)
                level = 'block'

        else: #BlockReplica
            deletion_list.append(replica.block)
            level = 'block'

        self._run_deletion_request(request_mapping, replica.site, level, deletion_list, comments)

        return request_mapping

    def schedule_deletions(self, replica_list, comments = ''): #override
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        for replica in replica_list:
            replicas_by_site[replica.site].append(replica)

            if replica.site.storage_type == Site.TYPE_MSS and not self.allow_tape_deletion:
                LOG.warning('Deletion from MSS not allowed by configuration.')
                return {}

        for site, replica_list in replicas_by_site.iteritems():
            # execute the deletions in two steps: one for dataset-level and one for block-level
            deletion_lists = {'dataset': [], 'block': []}

            for replica in replica_list:
                if type(replica) is DatasetReplica:
                    blocks = set(r.block for r in replica.block_replicas)

                    if blocks == replica.dataset.blocks:
                        deletion_lists['dataset'].append(replica.dataset)
                    else:
                        deletion_lists['block'].extend(blocks)

                else: #BlockReplica
                    deletion_lists['block'].append(replica.block)

            self._run_deletion_request(request_mapping, site, 'dataset', deletion_lists['dataset'], comments)
            self._run_deletion_request(request_mapping, site, 'block', deletion_lists['block'], comments)

        return request_mapping

    def _run_deletion_request(self, request_mapping, site, level, deletion_list, comments):
        full_catalog = collections.defaultdict(list)

        if level == 'dataset':
            for dataset in deletion_list:
                full_catalog[dataset] = []
        elif level == 'block':
            for block in deletion_list:
                full_catalog[block.dataset].append(block)

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

            if chunk_size < self.deletion_chunk_size and len(full_catalog) != 0:
                continue

            options = {
                'node': site.name,
                'data': self._phedex.form_catalog_xml(request_catalog),
                'level': level,
                'rm_subscriptions': 'y',
                'comments': comments
            }
    
            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response) if successful
            if self.dry_run:
                result = [{'id': '%d' % self._next_operation_id}]
                self._next_operation_id += 1
            else:
                try:
                    result = self._phedex.make_request('delete', options, method = POST)
                except:
                    if self._phedex.last_errorcode == 400:
                        # Sometimes we have invalid data in the list of objects to delete.
                        # PhEDEx throws a 400 error in such a case. We have to then try to identify the
                        # problematic item through trial and error.
                        if len(items) == 1:
                            LOG.error('Could not delete %s from %s', str(items[0]), site.name)
                            result = []
                        else:
                            self._run_deletion_request(request_mapping, site, level, items[:len(items) / 2], comments)
                            self._run_deletion_request(request_mapping, site, level, items[len(items) / 2:], comments)
                    else:
                        result = []

            if len(result) != 0:
                request_id = int(result[0]['id']) # return value is a string
                LOG.warning('PhEDEx deletion request id: %d', request_id)

                approved = False

                if self.dry_run:
                    approved = True

                elif self.auto_approval:
                    try:
                        result = self._phedex.make_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)
                    except:
                        LOG.error('deletion approval of request %d failed.', request_id)
                    else:
                        approved = True

                request_mapping[request_id] = (approved, site, items)

            else:
                LOG.error('Deletion %s failed.', str(options))
                # we should probably do something here

            request_catalog = {}
            chunk_size = 0
            items = []

    def deletion_status(self, request_id): #override
        request = self._phedex.make_request('deleterequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        node_info = request[0]['nodes']['node'][0]
        site_name = node_info['name']
        last_update = node_info['decided_by']['time_decided']

        status = {}
        for ds_entry in request[0]['data']['dbs']['dataset']:
            status[ds_entry['name']] = (ds_entry['bytes'], ds_entry['bytes'], last_update)
            
        return status
