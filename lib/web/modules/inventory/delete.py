import time
import logging

from dynamo.web.exceptions import MissingParameter, IllFormedRequest, InvalidRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class DeleteData(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.write_enabled = True

    def run(self, caller, request, inventory):
        """
        Delete data from inventory with a JSON (sent via POST. Data must be a dict.
        The top level keys must be "dataset", "site", "group", or "datasetreplica".
        The values must be lists of dicts, where each dict represents an object. Having unique
        names are sufficient to identify the objects. Blocks should be listed
        within a dataset dict, Files within Blocks, and BlockReplicas within DatasetReplicas.
        """

        if ('admin', 'inventory') not in caller.authlist:
            raise AuthorizationError()

        if type(request) is not dict:
            raise IllFormedRequest('request', type(request).__name__, hint = 'data must be a dict type')

        counts = {}

        if 'dataset' in request:
            self._delete_datasets(request['dataset'], inventory, counts)

        if 'site' in request:
            self._delete_sites(request['site'], inventory, counts)

        if 'group' in request:
            self._delete_groups(request['group'], inventory, counts)

        if 'datasetreplica' in request:
            self._delete_datasetreplicas(request['datasetreplica'], inventory, counts)

        return counts

    def _delete_datasets(self, objects, inventory, counts):
        num_datasets = 0

        for obj in objects:
            try:
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'dataset ' + str(obj))

            try:
                dataset = inventory.datasets[name]
            except KeyError:
                raise InvalidRequest('Unknown dataset %s' % name)

            if 'blocks' in obj:
                # block-level deletion
                self._delete_blocks(obj['blocks'], dataset, inventory, counts)
            except KeyError:
                try:
                    inventory.delete(dataset)
                except:
                    raise RuntimeError('Inventory update failed')

                num_datasets += 1

        counts['datasets'] = num_datasets

    def _delete_sites(self, objects, inventory, counts):
        num_sites = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'site ' + str(obj))

            try:
                site = inventory.sites[name]
            except KeyError:
                raise InvalidRequest('Unknown site %s' % name)

            try:
                inventory.delete(site)
            except:
                raise RuntimeError('Inventory update failed')
        
            num_sites += 1

        counts['sites'] = num_sites

    def _delete_groups(self, objects, inventory, counts):
        num_groups = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'group ' + str(obj))

            try:
                group = inventory.groups[name]
            except KeyError:
                raise InvalidRequest('Unknown group %s' % name)

            try:
                inventory.delete(group)
            except:
                raise RuntimeError('Inventory update failed')

            num_groups += 1

        counts['groups'] = num_groups

    def _delete_datasetreplicas(self, objects, inventory, counts):
        num_datasetreplicas = 0

        for obj in objects:
            try:
                dataset_name = obj['dataset']
            except KeyError:
                raise MissingParameter('dataset', context = 'datasetreplica ' + str(obj))
            else:
                try:
                    dataset = inventory.datasets[dataset_name]
                except KeyError:
                    raise InvalidRequest('Unknown dataset %s' % dataset_name)

            try:
                site_name = obj['site']
            except KeyError:
                raise MissingParameter('site', context = 'datasetreplica ' + str(obj))
            else:
                try:
                    site = inventory.sites[site_name]
                except KeyError:
                    raise InvalidRequest('Unknown site %s' % site_name)

            replica = site.find_datast_replica(dataset)

            if replica is None:
                raise InvalidRequest('Replica of %s at %s does not exist' % (dataset_name, site_name))

            if 'blockreplicas' in obj:
                # blockreplica-level deletion
                self._delete_blockreplicas(obj['blockreplicas'], replica, inventory, counts)
            else:
                # datasetreplica-level deletion
                try:
                    inventory.delete(replica)
                except:
                    raise RuntimeError('Inventory update failed')
        
                num_datasetreplicas += 1

        counts['datasetreplicas'] = num_datasetreplicas

    def _delete_blocks(self, objects, dataset, inventory, counts):
        num_blocks = 0

        for obj in objects:
            try:
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'block ' + str(obj))

            try:
                internal_name = df.Block.to_internal_name(name)
            except:
                raise IllFormedRequest('name', name, hint = 'Name does not match the format')

            block = dataset.find_block(internal_name)

            if block is None:
                raise InvalidRequest('Unknown block %s of %s' % (name, dataset.name))

            if 'files' in obj:
                # file-level deletion
                self._delete_files(obj['files'], block, inventory, counts)
            else:
                # block-level deletion
                try:
                    inventory.delete(block)
                except:
                    raise RuntimeError('Inventory update failed')

                num_blocks += 1

        try:
            counts['blocks'] += num_blocks
        except KeyError:
            counts['blocks'] = num_blocks

    def _delete_files(self, objects, block, inventory, counts):
        num_files = 0

        updated_replicas = set()

        for obj in objects:
            try:
                lfn = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'file ' + str(obj))

            lfile = block.find_file(lfn)

            if lfile is None:
                raise InvalidRequest('Unknown file %s' % lfn)

            for replica in block.replicas:
                try:
                    # delete_file adjusts the replica size too
                    replica.delete_file(lfile)
                except ValueError:
                    # file is not in replica
                    pass
                else:
                    updated_replicas.add(replica)

            block.size -= lfile.size
            block.num_files -= 1

            try:
                inventory.delete(lfile)
            except:
                raise RuntimeError('Inventory update failed')

            num_files += 1

        if num_files != 0:
            inventory.register_update(block)

        for replica in updated_replicas:
            inventory.update(replica)

        try:
            counts['files'] += num_files
        except KeyError:
            counts['files'] = num_files

    def _delete_blockreplicas(self, objects, dataset_replica, inventory, counts):
        num_blockreplicas = 0

        dataset = dataset_replica.dataset
        site = dataset_replica.site

        for obj in objects:
            try:
                block_name = obj['block']
            except KeyError:
                raise MissingParameter('block', context = 'blockreplica ' + str(obj))

            block_internal_name = df.Block.to_internal_name(block_name)

            block = dataset.find_block(block_internal_name)

            if block is None:
                raise InvalidRequest('Unknown block %s' % block_name)

            block_replica = block.find_replica(site)

            if block_replica is None:
                raise InvalidRequest('Replica of %s at %s does not exist' % (block.full_name(), site.name))

            try:
                inventory.delete(block_replica)
            except:
                raise RuntimeError('Inventory update failed')

            num_blockreplicas += 1

        try:
            counts['blockreplicas'] += num_blockreplicas
        except KeyError:
            counts['blockreplicas'] = num_blockreplicas

# exported to __init__.py
export_data = {'delete': DeleteData}
