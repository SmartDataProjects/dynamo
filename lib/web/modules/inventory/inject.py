import json

from dynamo.web.exceptions import MissingParameter, IllFormedRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
import dynamo.dataformat as df

class InjectData(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.write_enabled = True

    def run(self, caller, request, inventory):
        """
        Inject data into inventory with a JSON (request field 'data'). Data must be a dict.
        The top level keys must be "dataset", "site", "group", or "datasetreplica".
        The values must be lists of dicts, where each dict represents an object. Blocks should be listed
        within a dataset dict, Files within Blocks, and BlockReplicas within DatasetReplicas.
        """

        if ('admin', 'inventory') not in caller.authlist:
            raise AuthorizationError()

        if type(request) is not dict:
            raise IllFormedRequest('request', type(request).__name__, hint = '"data" must be a dict type')

        counts = {}

        if 'dataset' in request:
            self._make_datasets(request['dataset'], inventory, counts)

        if 'site' in request:
            self._make_sites(request['site'], inventory, counts)

        if 'group' in request:
            self._make_groups(request['group'], inventory, counts)

        if 'datasetreplica' in request:
            self._make_datasetreplicas(request['datasetreplica'], inventory, counts)

        return counts

    def _make_datasets(self, objects, inventory, counts):
        num_datasets = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = str(obj))

            try:
                blocks = obj.pop('blocks')
            except KeyError:
                blocks = None

            try:
                dataset = df.Dataset(name, **obj)
            except TypeError:
                obj['name'] = name
                raise IllFormedRequest('dataset', str(obj))

            try:
                embedded_clone, updated = dataset.embed_into(inventory, check = True)
            except:
                raise RuntimeError('Inventory update failed')
    
            if updated:
                inventory.register_update(embedded_clone)
                num_datasets += 1

            if blocks is not None:
                self._make_blocks(objects, embedded_clone, inventory, counts)

        counts['datasets'] = num_datasets

    def _make_sites(self, objects, inventory, counts):
        num_sites = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = str(obj))

            try:
                site = df.Site(name, **obj)
            except TypeError:
                obj['name'] = name
                raise IllFormedRequest('site', str(obj))

            try:
                embedded_clone, updated = site.embed_into(inventory, check = True)
            except:
                raise RuntimeError('Inventory update failed')
    
            if updated:
                inventory.register_update(embedded_clone)
                num_sites += 1

        counts['sites'] = num_sites

    def _make_groups(self, objects, inventory, counts):
        num_groups = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = str(obj))

            try:
                group = df.Group(name, **obj)
            except TypeError:
                obj['name'] = name
                raise IllFormedRequest('group', str(obj))

            try:
                embedded_clone, updated = group.embed_into(inventory, check = True)
            except:
                raise RuntimeError('Inventory update failed')
    
            if updated:
                inventory.register_update(embedded_clone)
                num_groups += 1

        counts['groups'] = num_groups

    def _make_datasetreplicas(self, objects, inventory, counts):
        num_datasetreplicas = 0

        for obj in objects:
            try:
                dataset_name = obj.pop('dataset')
            except KeyError:
                raise MissingParameter('dataset', context = str(obj))

            try:
                site_name = obj.pop('site')
            except KeyError:
                obj['dataset'] = dataset_name
                raise MissingParameter('site', context = str(obj))

            try:
                blockreplicas = obj.pop('blockreplicas')
            except KeyError:
                blockreplicas = None

            try:
                group = obj.pop('group')
            except KeyError:
                group = None

            replica = df.DatasetReplica(dataset_name, site_name)

            try:
                embedded_clone, updated = replica.embed_into(inventory, check = True)
            except:
                raise RuntimeError('Inventory update failed')
    
            if updated:
                inventory.register_update(embedded_clone)
                num_datasetreplicas += 1

            if blockreplicas is not None:
                self._make_blockreplicas(blockreplicas, group, embedded_clone, inventory, counts)

        counts['datasetreplicas'] = num_datasetreplicas

    def _make_blocks(self, objects, dataset, counts):
        num_blocks = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = str(obj))

            try:
                internal_name = df.Block.to_internal_name(name)
            except:
                obj['name'] = name
                raise IllFormedRequest('block', str(obj), hint = 'Name does not match the format')

            try:
                files = obj.pop('files')
            except KeyError:
                files = None

            try:
                block = df.Block(internal_name, dataset = dataset, **obj)
            except TypeError:
                obj['name'] = name
                raise IllFormedRequest('block', str(obj))

            existing = dataset.find_block(internal_name)
            if existing == block:
                continue

            if existing is None:
                block._files = set()
                dataset.blocks.add(block)
                existing = block
            else:
                existing._copy_no_check(block)

            num_blocks += 1
            inventory.register_update(existing)

            if files is not None:
                self._make_files(objects, existing, inventory, counts)

        counts['blocks'] = num_blocks

    def _make_files(self, objects, block, inventory, counts):
        num_files = 0

        for obj in objects:
            try:
                lfn = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = str(obj))

            try:
                lfile = df.File(lfn, block = block, **obj)
            except TypeError:
                obj['name'] = lfn
                raise IllFormedRequest('file', str(obj))

            existing = block.find_file(lfn)
            if existing == lfile:
                continue

            if existing is None:
                block.files.add(lfile)
                existing = lfile
            else:
                existing._copy_no_check(lfile)

            num_files += 1
            inventory.register_update(existing)

        counts['files'] = num_files

    def _make_blockreplicas(self, objects, default_group, dataset_replica, inventory, counts):
        num_blockreplicas = 0

        dataset = dataset_replica.dataset
        site = dataset_replica.site

        for obj in objects:
            try:
                block_name = obj.pop('block')
            except KeyError:
                raise MissingParameter('block', context = str(obj))

            block_internal_name = df.Block.to_internal_name(block_name)

            block = dataset.find_block(block_internal_name)

            if block is None:
                obj['block'] = block_name
                raise IllFormedRequest('blockreplica', str(obj), hint = 'Unknown block %s' % block_name)

            try:
                group_name = obj.pop('group')
            except KeyError:
                group_name = default_group

            try:
                obj['group'] = inventory.groups[group_name]
            except KeyError:
                obj['block'] = block_name
                raise IllFormedRequest('blockreplica', str(obj), hint = 'Unknown group %s' % group_name)

            try:
                replica = df.BlockReplica(block, site, **obj)
            except TypeError:
                obj['block'] = block_name
                obj['group'] = group_name
                raise IllFormedRequest('blockreplica', str(obj))

            existing = block.find_replica(site)
            if existing == replica:
                continue

            if existing is None:
                dataset_replica.block_replicas.add(replica)
                block.replicas.add(replica)
                site.add_block_replica(replica)
                existing = replica
            else:
                existing._copy_no_check(replica)

            num_blockreplicas += 1
            inventory.register_update(existing)

        counts['blockreplicas'] = num_blockreplicas
        

# exported to __init__.py
export_data = {'inject': InjectData}
