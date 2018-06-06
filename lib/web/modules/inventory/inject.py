import time
import logging

from dynamo.web.exceptions import MissingParameter, IllFormedRequest, InvalidRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
from dynamo.fileop.rlfsm import RLFSM
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class InjectData(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)
        self.write_enabled = True

        self.rlfsm = RLFSM(config.rlfsm)

    def run(self, caller, request, inventory):
        """
        Inject data into inventory with a JSON (sent via POST). Data must be a dict.
        The top level keys must be "dataset", "site", "group", or "datasetreplica".
        The values must be lists of dicts, where each dict represents an object. Blocks should be listed
        within a dataset dict, Files within Blocks, and BlockReplicas within DatasetReplicas.
        """

        if ('admin', 'inventory') not in caller.authlist:
            raise AuthorizationError()

        if type(request) is not dict:
            raise IllFormedRequest('request', type(request).__name__, hint = 'data must be a dict type')

        counts = {}

        if 'dataset' in request:
            blocks_with_new_file = self._make_datasets(request['dataset'], inventory, counts)
        else:
            blocks_with_new_file = []

        if 'site' in request:
            self._make_sites(request['site'], inventory, counts)

        if 'group' in request:
            self._make_groups(request['group'], inventory, counts)

        if 'datasetreplica' in request:
            self._make_datasetreplicas(request['datasetreplica'], inventory, counts)

        # Do this here to minimize the risk of creating invalid subscriptions
        for block in blocks_with_new_file:
            for replica in block.replicas:
                self.rlfsm.subscribe_files(replica)

        return counts

    def _make_datasets(self, objects, inventory, counts):
        num_datasets = 0

        blocks_with_new_file = []

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'dataset ' + str(obj))

            try:
                blocks = obj.pop('blocks')
            except KeyError:
                blocks = None

            try:
                dataset = inventory.datasets[name]
            except KeyError:
                # new dataset
                try:
                    obj.pop('did')
                except KeyError:
                    pass
    
                try:
                    obj['software_version'] = df.Dataset.format_software_version(obj['software_version'])
                except KeyError:
                    pass
    
                try:
                    new_dataset = df.Dataset(name, **obj)
                except TypeError as exc:
                    obj['name'] = name
                    raise IllFormedRequest('dataset', str(obj), hint = str(exc))
    
                try:
                    dataset = inventory.update(new_dataset)
                except:
                    raise RuntimeError('Inventory update failed')

                num_datasets += 1

            if blocks is not None:
                blocks_with_new_file.extend(self._make_blocks(blocks, dataset, inventory, counts))

        counts['datasets'] = num_datasets

        return blocks_with_new_file

    def _make_sites(self, objects, inventory, counts):
        num_sites = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'site ' + str(obj))

            if name not in inventory.sites:
                # new site
                try:
                    obj.pop('sid')
                except KeyError:
                    pass
    
                try:
                    new_site = df.Site(name, **obj)
                except TypeError as exc:
                    obj['name'] = name
                    raise IllFormedRequest('site', str(obj), hint = str(exc))
    
                try:
                    inventory.update(new_site)
                except:
                    raise RuntimeError('Inventory update failed')
        
                num_sites += 1

        counts['sites'] = num_sites

    def _make_groups(self, objects, inventory, counts):
        num_groups = 0

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'group ' + str(obj))

            if name not in inventory.groups:
                # new group
                try:
                    obj.pop('gid')
                except KeyError:
                    pass
    
                try:
                    new_group = df.Group(name, **obj)
                except TypeError as exc:
                    obj['name'] = name
                    raise IllFormedRequest('group', str(obj), hint = str(exc))
    
                try:
                    inventory.update(new_group)
                except:
                    raise RuntimeError('Inventory update failed')

                num_groups += 1

        counts['groups'] = num_groups

    def _make_datasetreplicas(self, objects, inventory, counts):
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
                obj['dataset'] = dataset_name
                raise MissingParameter('site', context = 'datasetreplica ' + str(obj))
            else:
                try:
                    site = inventory.sites[site_name]
                except KeyError:
                    raise InvalidRequest('Unknown site %s' % site_name)

            try:
                blockreplicas = obj['blockreplicas']
            except KeyError:
                blockreplicas = None

            replica = site.find_datast_replica(dataset)

            if replica is None:
                # new replica
                try:
                    group_name = obj['group']
                except KeyError:
                    group = df.Group.null_group
                else:
                    try:
                        group = inventory.groups[group_name]
                    except KeyError:
                        raise InvalidRequest('Unknown group %s' % group_name)

                if len(dataset.replicas) == 0:
                    # "origin" dataset replica cannot be growing
                    growing = False
                else:
                    try:
                        growing = obj['growing']
                    except KeyError:
                        growing = True
                    
                new_replica = df.DatasetReplica(dataset, site, growing = growing, group = group)
    
                try:
                    replica = inventory.update(new_replica)
                except:
                    raise RuntimeError('Inventory update failed')
        
                num_datasetreplicas += 1

            if blockreplicas is not None:
                self._make_blockreplicas(blockreplicas, replica, inventory, counts)

        counts['datasetreplicas'] = num_datasetreplicas

    def _make_blocks(self, objects, dataset, inventory, counts):
        num_blocks = 0

        blocks_with_new_file = []

        for obj in objects:
            try:
                name = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'block ' + str(obj))

            try:
                internal_name = df.Block.to_internal_name(name)
            except:
                obj['name'] = name
                raise IllFormedRequest('name', name, hint = 'Name does not match the format')

            try:
                files = obj.pop('files')
            except KeyError:
                files = None

            block = dataset.find_block(internal_name)

            if block is None:
                # new block
                new_block = True

                # size and num_files to be set through make_files
                obj['size'] = 0
                obj['num_files'] = 0

                try:
                    new_block = df.Block(internal_name, dataset = dataset, **obj)
                except TypeError as exc:
                    obj['name'] = name
                    raise IllFormedRequest('block', str(obj), hint = str(exc))

                try:
                    block = inventory.update(new_block)
                except:
                    raise RuntimeError('Inventory update failed')
    
                block._files = set()

                for replica in dataset.replicas:
                    if replica.growing:
                        # For growing replicas, we automatically create new block replicas
                        blockreplica = {'block': block.real_name(), 'site': replica.site.name, 'group': replica.group.name, 'size': 0, 'last_update': time.time()}
                        self._make_blockreplicas([blockreplica], replica, inventory, counts)

                num_blocks += 1

            else:
                new_block = False

            if files is not None:
                added_new_file = self._make_files(files, block, new_block, inventory, counts)
                if added_new_file and not new_block:
                    blocks_with_new_file.append(block)

        try:
            counts['blocks'] += num_blocks
        except KeyError:
            counts['blocks'] = num_blocks

        return blocks_with_new_file

    def _make_files(self, objects, block, new_block, inventory, counts):
        num_files = 0

        block_replicas = {}

        for obj in objects:
            try:
                lfn = obj.pop('name')
            except KeyError:
                raise MissingParameter('name', context = 'file ' + str(obj))

            lfile = block.find_file(lfn)

            if lfile is None:
                # new file
                try:
                    new_lfile = df.File(lfn, block = block, **obj)
                except TypeError as exc:
                    obj['name'] = lfn
                    raise IllFormedRequest('file', str(obj), hint = str(exc))

                if not new_block:
                    # when adding a file to an existing block, we need to specify where the file can be found
                    # otherwise subscriptions made for other replicas will never complete
                    try:
                        site_name = obj['site']
                    except KeyError:
                        raise MissingParameter('site', context = 'file ' + str(obj))

                    try:
                        block_replica = block_replicas[site_name]
                    except KeyError:
                        try:
                            site = inventory.sites[site_name]
                        except KeyError:
                            raise InvalidRequest('Unknown site %s' % site_name)
                        
                        block_replica = block.find_replica(site)
                        if block_replica is None:
                            raise InvalidRequest('Block %s does not have a replica at %s' % (block.full_name(), site_name))

                        block_replicas[site_name] = block_replica

                block.size += new_lfile.size
                block.num_files += 1

                try:
                    lfile = inventory.update(new_lfile)
                except:
                    raise RuntimeError('Inventory update failed')

                if not new_block:
                    block_replica.size += lfile.size

                    if block_replica.file_ids is None:
                        pass
                    else:
                        block_replica.file_ids += (lfile.lfn,)
    
                num_files += 1

        if num_files != 0:
            inventory.register_update(block)

        try:
            for block_replica in block_replicas.itervalues():
                inventory.update(block_replica)
        except:
            raise RuntimeError('Inventory update failed')

        try:
            counts['files'] += num_files
        except KeyError:
            counts['files'] = num_files

        return num_files != 0

    def _make_blockreplicas(self, objects, dataset_replica, inventory, counts):
        num_blockreplicas = 0

        dataset = dataset_replica.dataset
        site = dataset_replica.site

        for obj in objects:
            try:
                block_name = obj.pop('block')
            except KeyError:
                raise MissingParameter('block', context = 'blockreplica ' + str(obj))

            block_internal_name = df.Block.to_internal_name(block_name)

            block = dataset.find_block(block_internal_name)

            if block is None:
                raise InvalidRequest('Unknown block %s' % block_name)

            block_replica = block.find_replica(site)

            if block_replica is None:
                # new replica
                try:
                    group_name = obj.pop('group')
                except KeyError:
                    group = dataset_replica.group
                else:
                    try:
                        group = inventory.groups[group_name]
                    except KeyError:
                        raise InvalidRequest('Unknown group %s' % group_name)

                if len(block.replicas) == 0:
                    # "origin" block replicas must always be full
                    obj['file_ids'] = None
                    obj['size'] = -1 # will set size to block size
    
                try:
                    new_replica = df.BlockReplica(block, site, group, **obj)
                except TypeError as exc:
                    obj['block'] = block_name
                    obj['group'] = group_name
                    raise IllFormedRequest('blockreplica', str(obj), hint = str(exc))

                try:
                    inventory.update(new_replica)
                except:
                    raise RuntimeError('Inventory update failed')

                num_blockreplicas += 1

        try:
            counts['blockreplicas'] += num_blockreplicas
        except KeyError:
            counts['blockreplicas'] = num_blockreplicas

# exported to __init__.py
export_data = {'inject': InjectData}
