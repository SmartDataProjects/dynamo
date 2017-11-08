import logging
import collections

from operation.copy import CopyInterface
from operation.deletion import DeletionInterface
from common.interface.webservice import RESTService, GET, POST

LOG = logging.getLogger(__name__)

class PhEDEx(CopyInterface, DeletionInterface):
    """Copy and Deletion using PhEDEx."""

    def __init__(self, config):
        CopyInterface.__init__(self)
        DeletionInterface.__init__(self)

        self._phedex_interface = RESTService(config.url_base, use_cache = True)

    def schedule_copy(self, dataset_replica, group, comments = '', is_test = False): #override (CopyInterface)
        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        dataset = dataset_replica.dataset
        replica_blocks = [r.block for r in dataset_replica.block_replicas]

        # shouldn't pass datasets with blocks not loaded though..
        if dataset.blocks is not None and set(replica_blocks) == set(dataset.blocks):
            catalogs[dataset] = []
            level = 'dataset'
        else:
            catalogs[dataset] = replica_blocks
            level = 'block'

        options = {
            'node': dataset_replica.site.name,
            'data': self._form_catalog_xml(catalogs),
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

        LOG.info('schedule_copy  subscribe %d datasets at %s', len(catalogs), options['node'])
        if LOG.getEffectiveLevel() == logging.DEBUG:
            LOG.debug('schedule_copy  subscribe: %s', str(options))

        if config.read_only:
            return

        if is_test:
            return -1

        else:
            try:
                result = self._make_phedex_request('subscribe', options, method = POST)
            except:
                result = []
    
            if len(result) == 0:
                LOG.error('schedule_copy failed.')
                return 0
    
            return int(result[0]['id'])

    def schedule_copies(self, replicas, group, comments = '', is_test = False): #override (CopyInterface)
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        for replica in replicas:
            replicas_by_site[replica.site].append(replica)

        for site, replica_list in replicas_by_site.iteritems():
            subscription_chunk = []
            chunk_size = 0
            for replica in replica_list:
                subscription_chunk.append(replica)
                if type(replica) is DatasetReplica:
                    chunk_size += replica.size(physical = False)
                elif type(replica) is BlockReplica:
                    chunk_size += replica.block.size

                if chunk_size >= config.phedex.subscription_chunk_size or replica == replica_list[-1]:
                    self._run_subscription_request(request_mapping, site, group, subscription_chunk, comments, is_test)
                    subscription_chunk = []
                    chunk_size = 0

        return request_mapping

    def _run_subscription_request(self, request_mapping, site, group, replica_list, comments, is_test):
        # replica_list can contain DatasetReplica and BlockReplica mixed

        catalogs = collections.defaultdict(list)

        level = 'dataset'

        for replica in replica_list:
            if type(replica) is DatasetReplica:
                dataset = replica.dataset
                replica_blocks = [r.block for r in replica.block_replicas]

                if dataset.blocks is not None and set(replica_blocks) == set(dataset.blocks):
                    catalogs[dataset] = []
                else:
                    catalogs[dataset].extend(replica_blocks)
                    level = 'block'

            elif type(replica) is BlockReplica:
                catalogs[replica.block.dataset].append(replica.block)
                level = 'block'

        options = {
            'node': site.name,
            'data': self._form_catalog_xml(catalogs),
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

        LOG.info('schedule_copies  subscribe %d datasets at %s', len(catalogs), options['node'])
        if LOG.getEffectiveLevel() == logging.DEBUG:
            LOG.debug('schedule_copies  subscribe: %s', str(options))

        if config.read_only:
            return

        if is_test:
            request_id = -1
            while request_id in request_mapping:
                request_id -= 1

            request_mapping[request_id] = (True, replica_list)

        else:
            # result = [{'id': <id>}] (item 'request_created' of PhEDEx response)
            try:
                result = self._make_phedex_request('subscribe', options, method = POST)
            except:
                result = []

            if len(result) == 0:
                LOG.error('schedule_copies  copy failed.')
                return

            request_id = int(result[0]['id']) # return value is a string

            LOG.warning('PhEDEx subscription request id: %d', request_id)
            
            request_mapping[request_id] = (True, replica_list)

    def schedule_reassignments(self, replicas, group, comments = '', is_test = False): #override (CopyInterface)
        # for PhEDEx, copying and ownership reassignment are the same thing
        self.schedule_copies(replicas, group, comments, is_test)

    def schedule_deletion(self, replica, comments = '', is_test = False): #override (DeletionInterface)
        if replica.site.storage_type == Site.TYPE_MSS and config.daemon_mode:
            LOG.warning('Deletion from MSS cannot be done in daemon mode.')
            return None

        catalogs = {} # {dataset: [block]}. Content can be empty if inclusive deletion is desired.

        if type(replica) == DatasetReplica:
            replica_blocks = [r.block for r in replica.block_replicas]

            if replica.dataset.blocks is not None and set(replica_blocks) == set(replica.dataset.blocks):
                catalogs[replica.dataset] = []
                level = 'dataset'
            else:
                catalogs[replica.dataset] = replica_blocks
                level = 'block'

        elif type(replica) == BlockReplica:
            catalogs[replica.block.dataset] = [replica.block]
            level = 'block'

        options = {
            'node': replica.site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': level,
            'rm_subscriptions': 'y',
            'comments': comments
        }

        if config.read_only:
            LOG.info('schedule_deletion  delete %d datasets', len(catalogs))
            LOG.debug('schedule_deletion  delete: %s', str(options))
            return None

        if is_test:
            LOG.info('schedule_deletion  delete %d datasets', len(catalogs))
            LOG.debug('schedule_deletion  delete: %s', str(options))
            return (-1, True, [replica])

        else:
            try:
                result = self._make_phedex_request('delete', options, method = POST)
            except:
                LOG.error('schedule_deletion  delete failed.')
                return (0, False, [])

            request_id = int(result[0]['id']) # return value is a string

            LOG.warning('PhEDEx deletion request id: %d', request_id)

            return_value = (request_id, False, [replica])

            if config.phedex.auto_approve_deletions:
                try:
                    result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': replica.site.name}, method = POST)
                    return_value = (request_id, True, [replica])
                except:
                    LOG.error('schedule_deletions  deletion approval failed.')

            return return_value

    def schedule_deletions(self, replica_list, comments = '', is_test = False): #override (DeletionInterface)
        request_mapping = {}

        replicas_by_site = collections.defaultdict(list)
        has_mss = False
        for replica in replica_list:
            replicas_by_site[replica.site].append(replica)
            if replica.site.storage_type == Site.TYPE_MSS:
                has_mss = True

        if has_mss and config.daemon_mode:
            LOG.warning('Deletion from MSS cannot be done in daemon mode.')
            return {}

        for site, replica_list in replicas_by_site.iteritems():
            # execute the deletions in two steps: one for dataset-level and one for block-level
            deletion_lists = {'dataset': [], 'block': []}

            for replica in replica_list:
                replica_blocks = [r.block for r in replica.block_replicas]

                if replica.dataset.blocks is not None and set(replica_blocks) == set(replica.dataset.blocks):
                    deletion_lists['dataset'].append(replica)
                else:
                    deletion_lists['block'].append(replica)

            self._run_deletion_request(request_mapping, site, 'dataset', deletion_lists['dataset'], comments, is_test)
            self._run_deletion_request(request_mapping, site, 'block', deletion_lists['block'], comments, is_test)

        return request_mapping

    def _run_deletion_request(self, request_mapping, site, level, deletion_list, comments, is_test):
        """
        Sometimes we have invalid data in the list of objects to delete.
        PhEDEx throws a 400 error in such a case. We have to then try to identify the
        problematic item through trial and error.
        """

        catalogs = {}
        for replica in deletion_list:
            if level == 'dataset':
                catalogs[replica.dataset] = []
            elif level == 'block':
                catalogs[replica.dataset] = [r.block for r in replica.block_replicas]

        if len(catalogs) == 0:
            return

        options = {
            'node': site.name,
            'data': self._form_catalog_xml(catalogs),
            'level': level,
            'rm_subscriptions': 'y',
            'comments': comments
        }

        if config.read_only:
            LOG.info('schedule_deletions  delete %d datasets', len(catalogs))
            LOG.debug('schedule_deletions  delete: %s', str(options))
            return

        if is_test:
            LOG.info('schedule_deletions  delete %d datasets', len(catalogs))
            LOG.debug('schedule_deletions  delete: %s', str(options))
            request_id = -1
            while request_id in request_mapping:
                request_id -= 1

            request_mapping[request_id] = (True, deletion_list)
            return

        # result = [{'id': <id>}] (item 'request_created' of PhEDEx response) if successful
        try:
            result = self._make_phedex_request('delete', options, method = POST)
        except:
            if self._phedex_interface.last_errorcode == 400:
                # bad request - split the deletion list and try each half
                if len(deletion_list) == 1:
                    LOG.error('schedule_deletions  Could not delete %s from %s', replica.dataset.name, site.name)
                else:
                    call_deletion(site, level, deletion_list[:len(deletion_list) / 2])
                    call_deletion(site, level, deletion_list[len(deletion_list) / 2:])
            else:
                LOG.error('schedule_deletions  Could not delete %d datasets from %s', len(deletion_list), site.name)
                
            return

        request_id = int(result[0]['id']) # return value is a string
    
        request_mapping[request_id] = (False, deletion_list) # (completed, deleted_replicas)
    
        LOG.warning('PhEDEx deletion request id: %d', request_id)

        if config.phedex.auto_approve_deletions:
            try:
                result = self._make_phedex_request('updaterequest', {'decision': 'approve', 'request': request_id, 'node': site.name}, method = POST)
                request_mapping[request_id] = (True, deletion_list)
            except:
                LOG.error('schedule_deletions  deletion approval failed.')

    def copy_status(self, request_id): #override (CopyInterface)
        request = self._make_phedex_request('transferrequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        site_name = request[0]['destinations']['node'][0]['name']

        dataset_names = []
        for ds_entry in request[0]['data']['dbs']['dataset']:
            dataset_names.append(ds_entry['name'])

        block_names = []
        for ds_entry in request[0]['data']['dbs']['block']:
            block_names.append(ds_entry['name'].replace('#', '%23'))

        subscriptions = []

        if len(dataset_names) != 0:
            chunks = [dataset_names[i:i + 35] for i in xrange(0, len(dataset_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._make_phedex_request('subscriptions', ['node=%s' % site_name] + ['dataset=%s' % n for n in chunk]))

        if len(block_names) != 0:
            chunks = [block_names[i:i + 35] for i in xrange(0, len(block_names), 35)]
            for chunk in chunks:
                subscriptions.extend(self._make_phedex_request('subscriptions', ['node=%s' % site_name] + ['block=%s' % n for n in chunk]))

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

    def deletion_status(self, request_id): #override (DeletionInterface)
        request = self._make_phedex_request('deleterequests', 'request=%d' % request_id)
        if len(request) == 0:
            return {}

        node_info = request[0]['nodes']['node'][0]
        site_name = node_info['name']
        last_update = node_info['decided_by']['time_decided']

        status = {}
        for ds_entry in request[0]['data']['dbs']['dataset']:
            status[ds_entry['name']] = (ds_entry['bytes'], ds_entry['bytes'], last_update)
            
        return status
