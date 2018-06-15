import time
import logging

from dynamo.web.exceptions import MissingParameter, IllFormedRequest, InvalidRequest, AuthorizationError
from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin
import dynamo.dataformat as df

LOG = logging.getLogger(__name__)

class InjectDataBase(WebModule):
    """
    Parse a JSON uploaded by the user and form injection instructions.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

    def run(self, caller, request, inventory):
        """
        Create injection instructions from a JSON (sent via POST). Data must be a dict.
        The top level keys must be "dataset", "site", "group", or "datasetreplica".
        The values must be lists of dicts, where each dict represents an object. Blocks should be listed
        within a dataset dict, Files within Blocks, and BlockReplicas within DatasetReplicas.
        """

        if ('admin', 'inventory') not in caller.authlist:
            raise AuthorizationError()

        if type(request) is not dict:
            raise IllFormedRequest('request', type(request).__name__, hint = 'data must be a dict type')

        self._initialize()

        counts = {}

        # blocks_with_new_file list used in the synchronous version of this class

        if 'dataset' in request:
            self._make_datasets(request['dataset'], inventory, counts)

        if 'site' in request:
            self._make_sites(request['site'], inventory, counts)

        if 'group' in request:
            self._make_groups(request['group'], inventory, counts)

        if 'datasetreplica' in request:
            self._make_datasetreplicas(request['datasetreplica'], inventory, counts)

        self._finalize()
        
        return counts

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    def _make_datasets(self, objects, inventory, counts):
        num_datasets = 0

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
                    dataset = self._update(inventory, new_dataset)
                except:
                    raise RuntimeError('Inventory update failed')

                num_datasets += 1

            if blocks is not None:
                self._make_blocks(blocks, dataset, inventory, counts)

        counts['datasets'] = num_datasets

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
                    self._update(inventory, new_site)
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
                    self._update(inventory, new_group)
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

            replica = site.find_dataset_replica(dataset)

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
                    replica = self._update(inventory, new_replica)
                except:
                    raise RuntimeError('Inventory update failed')
        
                num_datasetreplicas += 1

            if blockreplicas is not None:
                self._make_blockreplicas(blockreplicas, replica, inventory, counts)

        counts['datasetreplicas'] = num_datasetreplicas

    def _make_blocks(self, objects, dataset, inventory, counts):
        num_blocks = 0

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
                    block = self._update(inventory, new_block)
                except:
                    raise RuntimeError('Inventory update failed')
    
                block._files = set()

                for replica in dataset.replicas:
                    if replica.growing:
                        # For growing replicas, we automatically create new block replicas
                        # All new files will be subscribed to block replicas that don't have them yet
                        blockreplica = {'block': block.real_name(), 'site': replica.site.name, 'group': replica.group.name, 'size': 0, 'last_update': time.time()}
                        self._make_blockreplicas([blockreplica], replica, inventory, counts)

                num_blocks += 1

            else:
                new_block = False

            if files is not None:
                self._make_files(files, block, new_block, inventory, counts)

        try:
            counts['blocks'] += num_blocks
        except KeyError:
            counts['blocks'] = num_blocks

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
                    lfile = self._update(inventory, new_lfile)
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
            self._register_update(inventory, block)

        try:
            for block_replica in block_replicas.itervalues():
                self._update(inventory, block_replica)
        except:
            raise RuntimeError('Inventory update failed')

        try:
            counts['files'] += num_files
        except KeyError:
            counts['files'] = num_files

    def _make_blockreplicas(self, objects, dataset_replica, inventory, counts):
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
                # new replica
                try:
                    group_name = obj['group']
                except KeyError:
                    if dataset_replica.group is df.Group.null_group:
                        raise MissingParameter('group', context = 'blockreplica ' + str(obj))

                    group = dataset_replica.group
                else:
                    try:
                        group = inventory.groups[group_name]
                    except KeyError:
                        raise InvalidRequest('Unknown group %s' % group_name)

                kwd = {}
                
                if 'is_custodial' in obj:
                    kwd['is_custodial'] = obj['is_custodial']

                if 'last_update' in obj:
                    kwd['last_update'] = obj['last_update']

                if len(block.replicas) == 0:
                    # "origin" block replicas must always be full
                    kwd['file_ids'] = None
                    kwd['size'] = -1 # will set size to block size
                else:
                    if 'files' in obj:
                        if df.BlockReplica._use_file_ids:
                            kwd['file_ids'] = obj['files']
                        else:
                            kwd['file_ids'] = len(obj['files'])

                        size = 0
                        for lfn in obj['files']:
                            lfile = block.find_file(lfn)
                            if lfile is None:
                                raise InvalidRequest('Unknown file %s' % lfn)
                            size += lfile.size

                        kwd['size'] = size

                        if kwd['size'] == block.size:
                            if df.BlockReplica._use_file_ids:
                                if len(kwd['file_ids']) == block.num_files:
                                    kwd.pop('size')
                                    kwd.pop('file_ids')
                            else:
                                if kwd['file_ids'] == block.num_files:
                                    kwd.pop('size')
                                    kwd.pop('file_ids')

                try:
                    new_replica = df.BlockReplica(block, site, group, **kwd)
                except TypeError as exc:
                    raise IllFormedRequest('blockreplica', str(obj), hint = str(exc))

                try:
                    self._update(inventory, new_replica)
                except:
                    raise RuntimeError('Inventory update failed')

                num_blockreplicas += 1

        try:
            counts['blockreplicas'] += num_blockreplicas
        except KeyError:
            counts['blockreplicas'] = num_blockreplicas


class InjectData(InjectDataBase, MySQLRegistryMixin):
    """
    Asynchronous version of the injection. Injection instructions are queued in a registry table with the same format
    as the central inventory update table. The updater process will pick up the injection instructions asynchronously.
    """

    def __init__(self, config):
        InjectDataBase.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)

        self.inject_queue = []

    def _update(self, inventory, obj):
        embedded_clone, updated = obj.embed_into(inventory, check = True)
        if updated:
            self.inject_queue.append(embedded_clone)

        return embedded_clone

    def _register_update(self, inventory, obj):
        self.inject_queue.append(obj)

    def _initialize(self):
        # Lock will be release if this process crashes
        self.registry.lock_tables(write = ['data_injections'])

    def _finalize(self):
        fields = ('cmd', 'obj')
        mapping = lambda obj: ('update', repr(obj))

        self.registry.insert_many('data_injections', fields, mapping, self.inject_queue)
        self.registry.unlock_tables()

        self.message = 'Data will be injected in the regular update cycle later.'

class InjectDataSync(InjectDataBase):
    """
    Synchronous version of data injection. The client is responsible for retrying when injection fails due to
    an ongoing inventory update.
    """

    def __init__(self, config):
        InjectDataBase.__init__(self, config)

        # list of blocks whose subscriptions should be updated
        self.blocks_with_new_file = set()
        # using RLFSM to update the subscriptions
        # we use the default settings
        self.rlfsm = RLFSM()

        self.write_enabled = True

    def _update(self, inventory, obj):
        if type(obj) is df.File:
            self.blocks_with_new_file.add(obj.block)

        return inventory.update(obj)

    def _register_update(self, inventory, obj):
        inventory.register_update(obj)

    def _finalize(self):
        # Do this here to minimize the risk of creating invalid subscriptions
        for block in self.blocks_with_new_file:
            all_files = block.files
            for replica in block.replicas:
                self.rlfsm.subscribe_files(replica.site, all_files - replica.files())

        self.message = 'Data is injected.'

# exported to __init__.py
export_data = {
    'inject': InjectData,
    'injectsync': InjectDataSync
}
