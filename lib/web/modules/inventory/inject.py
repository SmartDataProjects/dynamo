import time
import logging

from dynamo.web.exceptions import MissingParameter, IllFormedRequest, InvalidRequest, AuthorizationError, TryAgain
from dynamo.web.modules._base import WebModule
import dynamo.dataformat as df
from dynamo.registry.registry import RegistryDatabase

LOG = logging.getLogger(__name__)

class InjectDataBase(WebModule):
    """
    Parse a JSON uploaded by the user and form injection instructions.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        self.must_authenticate = True

    def run(self, caller, request, inventory):
        """
        Create injection instructions from a JSON (sent via POST). Data must be a dict.
        The top level keys must be "dataset", "site", "group", or "datasetreplica".
        The values must be lists of dicts, where each dict represents an object. Blocks should be listed
        within a dataset dict, Files within Blocks, and BlockReplicas within DatasetReplicas.
        """

        if ('admin', 'inventory') not in caller.authlist:
            raise AuthorizationError()

        if type(self.input_data) is not dict:
            raise IllFormedRequest('input', type(self.input_data).__name__, hint = 'data must be a dict type')

        counts = {}

        # blocks_with_new_file list used in the synchronous version of this class

        if 'dataset' in self.input_data:
            self._make_datasets(self.input_data['dataset'], inventory, counts)

        if 'site' in self.input_data:
            self._make_sites(self.input_data['site'], inventory, counts)

        if 'group' in self.input_data:
            self._make_groups(self.input_data['group'], inventory, counts)

        if 'datasetreplica' in self.input_data:
            self._make_datasetreplicas(self.input_data['datasetreplica'], inventory, counts)

        self._finalize()
        
        return counts

    def _finalize(self):
        pass

    def _make_datasets(self, objects, inventory, counts):
        num_datasets = 0

        for obj in objects:
            try:
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'dataset ' + str(obj))

            try:
                blocks = obj['blocks']
            except KeyError:
                blocks = None

            try:
                dataset = inventory.datasets[name]
            except KeyError:
                # new dataset

                if 'status' not in obj:
                    obj['status'] = 'unknown'

                if 'data_type' not in obj:
                    obj['data_type'] = 'unknown'

                try:
                    obj['software_version'] = df.Dataset.format_software_version(obj['software_version'])
                except KeyError:
                    obj['software_version'] = None

                try:
                    new_dataset = df.Dataset(
                        name,
                        status = obj['status'],
                        data_type = obj['data_type'],
                        software_version = obj['software_version'],
                        last_update = int(time.time())
                    )
                except TypeError as exc:
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
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'site ' + str(obj))

            if name not in inventory.sites:
                # new site
                
                if 'host' not in obj:
                    obj['host'] = ''

                if 'storage_type' not in obj:
                    obj['storage_type'] = 'unknown'

                if 'backend' not in obj:
                    obj['backend'] = ''

                if 'status' not in obj:
                    obj['status'] = 'unknown'
    
                try:
                    new_site = df.Site(
                        name,
                        host = obj['host'],
                        storage_type = obj['storage_type'],
                        backend=  obj['backend'],
                        status = obj['status']
                    )
                except TypeError as exc:
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
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'group ' + str(obj))

            if name not in inventory.groups:
                # new group

                if 'olevel' not in obj:
                    obj['olevel'] = 'Block'
    
                try:
                    new_group = df.Group(
                        name,
                        obj['olevel']
                    )
                except TypeError as exc:
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

                if len(dataset.replicas) == 0:
                    # "origin" dataset replica cannot be growing
                    growing = False
                else:
                    try:
                        growing = obj['growing']
                    except KeyError:
                        growing = True

                if growing:
                    try:
                        group_name = obj['group']
                    except KeyError:
                        group = df.Group.null_group
                    else:
                        try:
                            group = inventory.groups[group_name]
                        except KeyError:
                            raise InvalidRequest('Unknown group %s' % group_name)
    
                    if group is df.Group.null_group:
                        raise InvalidRequest('Growing dataset replica %s:%s needs a group' % (site.name, dataset.name))

                else:
                    group = df.Group.null_group
                    
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
                name = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'block ' + str(obj))

            try:
                internal_name = df.Block.to_internal_name(name)
            except:
                obj['name'] = name
                raise IllFormedRequest('name', name, hint = 'Name does not match the format')

            try:
                files = obj['files']
            except KeyError:
                files = None

            block = dataset.find_block(internal_name)

            if block is None:
                # new block

                try:
                    # size and num_files to be set through make_files
                    block = df.Block(
                        internal_name,
                        dataset,
                        size = 0,
                        num_files = 0,
                        last_update = int(time.time())
                    )
                except TypeError as exc:
                    raise IllFormedRequest('block', str(obj), hint = str(exc))

                try:
                    block = self._update(inventory, block)
                except:
                    raise RuntimeError('Inventory update failed')
    
                block._files = set()

                # For growing dataset replicas, we automatically create new block replicas
                # All new files will be subscribed to block replicas that don't have them yet
                for replica in dataset.replicas:
                    if replica.growing:
                        block_replica_data = {'block': block.real_name(), 'site': replica.site.name, 'group': replica.group.name, 'size': 0}
                        self._make_blockreplicas([block_replica_data], replica, inventory, counts)

                num_blocks += 1

            if files is not None:
                self._make_files(files, block, inventory, counts)

        try:
            counts['blocks'] += num_blocks
        except KeyError:
            counts['blocks'] = num_blocks

    def _make_files(self, objects, block, inventory, counts):
        num_files = 0

        block_replicas = {}

        for obj in objects:
            try:
                lfn = obj['name']
            except KeyError:
                raise MissingParameter('name', context = 'file ' + str(obj))

            try:
                lfile = block.find_file(lfn)
            except df.IntegrityError:
                # If we are in the middle of an inventory update which involves adding files to this specific block,
                # the files may already be in the inventory store while the inventory image that the web server has
                # is from pre-update. When the block loads the files, it sees more files than it knows about, and
                # raises an IntegrityError.
                # The only solutions here are 1. Block web access while there is an update to File; 2. Raise a 503.
                # We choose 2.
                raise TryAgain('Inventory update is ongoing. Please retry after a minute.')

            if lfile is not None:
                continue

            # new file

            try:
                size = obj['size']
            except KeyError:
                raise MissingParameter('size', context = 'file ' + str(obj))

            try:
                checksum = tuple(obj[algo] for algo in df.File.checksum_algorithms)
            except KeyError:
                raise MissingParameter(','.join(df.File.checksum_algorithms), context = 'file ' + str(obj))

            try:
                new_lfile = df.File(
                    lfn,
                    block = block,
                    size = size,
                    checksum = checksum
                )

            except TypeError as exc:
                raise IllFormedRequest('file', str(obj), hint = str(exc))

            if 'site' in obj:
                site_name = obj['site']

                try:
                    block_replica = block_replicas[site_name]
                except KeyError:
                    try:
                        site = inventory.sites[site_name]
                    except KeyError:
                        raise InvalidRequest('Unknown site %s' % site_name)
                    
                    block_replica = block.find_replica(site)
                    if block_replica is None:
                        # replica doesn't exist yet; create one here

                        block_replica_data = {'block': block.real_name(), 'site': site.name, 'group': None, 'size': 0}

                        dataset_replica = block.dataset.find_replica(site)
                        if dataset_replica is None:
                            dataset_replica_data = {'dataset': block.dataset.name, 'site': site.name, 'blockreplicas': [block_replica_data], 'growing': False}
                            self._make_datasetreplicas([dataset_replica_data], inventory, counts)
                        else:
                            self._make_blockreplicas([block_replica_data], dataset_replica, inventory, counts)

                        block_replica = block.find_replica(site)

                    block_replicas[site_name] = block_replica

            else:
                if len(block.replicas) != 0:
                    # when adding a file to a block with replicas, we need to specify where the file can be found
                    # otherwise subscriptions made for other replicas will never complete
                    raise MissingParameter('site', context = 'file ' + str(obj))

                block_replica = None

            block.size += new_lfile.size
            block.num_files += 1

            try:
                lfile = self._update(inventory, new_lfile)
            except:
                raise RuntimeError('Inventory update failed')

            if block_replica is not None:
                # add_file updates the size and file_ids list
                block_replica.add_file(lfile)
                self._register_update(inventory, block_replica)

                # go through all the other replicas of this block and update the ones that claim to be full
                old_files_list = None

                for replica in block.replicas:
                    if replica is block_replica:
                        continue

                    if replica.file_ids is None:
                        if old_files_list is None:
                            # first time making the list
                            old_files_list = []

                            for f in block.files:
                                if f.lfn == lfile.lfn:
                                    continue
            
                                if f.id == 0:
                                    old_files_list.append(f.lfn)
                                else:
                                    old_files_list.append(f.id)
            
                            old_files_list = tuple(old_files_list)

                        replica.file_ids = old_files_list
                        self._register_update(inventory, replica)

            num_files += 1

        if num_files != 0:
            self._register_update(inventory, block)

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

                try:
                    block_replica = df.BlockReplica(
                        block,
                        site,
                        group,
                        last_update = int(time.time())
                    )
                except TypeError as exc:
                    raise IllFormedRequest('blockreplica', str(obj), hint = str(exc))

                num_blockreplicas += 1

                try:
                    block_replica = self._update(inventory, block_replica)
                except:
                    raise RuntimeError('Inventory update failed')

            if 'files' in obj:
                # add file to the block replica (no need to give the full list of files)
                if not df.BlockReplica._use_file_ids:
                    raise RuntimeError('File addition to block replica not supported in this Dynamo installation.')

                if block_replica.file_ids is None:
                    # the replica is already complete
                    continue

                file_ids = list(block_replica.file_ids)

                updated = False

                for lfn in obj['files']:
                    lfile = block.find_file(lfn)
                    if lfile is None:
                        raise InvalidRequest('Unknown file %s' % lfn)

                    if lfile.id == 0:
                        if lfn in file_ids:
                            continue
                        if lfn in set(f.lfn for f in block_replica.files()):
                            continue

                        file_ids.append(lfn)
                    else:
                        if lfile.id in file_ids:
                            continue

                        file_ids.append(lfile.id)

                    block_replica.size += lfile.size

                    updated = True

                if updated:
                    if len(file_ids) == block.num_files and block_replica.size == block.size:
                        block_replica.file_ids = None
                    else:
                        block_replica.file_ids = tuple(file_ids)

                    self._register_update(inventory, block_replica)

        try:
            counts['blockreplicas'] += num_blockreplicas
        except KeyError:
            counts['blockreplicas'] = num_blockreplicas


class InjectData(InjectDataBase):
    """
    Asynchronous version of the injection. Injection instructions are queued in a registry table with the same format
    as the central inventory update table. The updater process will pick up the injection instructions asynchronously.
    """

    def __init__(self, config):
        InjectDataBase.__init__(self, config)

        self.registry = RegistryDatabase()

        self.inject_queue = []

    def _update(self, inventory, obj):
        embedded_clone, updated = obj.embed_into(inventory, check = True)
        if updated:
            self.inject_queue.append(embedded_clone)

        return embedded_clone

    def _register_update(self, inventory, obj):
        self.inject_queue.append(obj)

    def _finalize(self):
        fields = ('cmd', 'obj')
        mapping = lambda obj: ('update', repr(obj))

        # make injection entries consecutive
        self.registry.db.lock_tables(write = ['data_injections'])
        self.registry.db.insert_many('data_injections', fields, mapping, self.inject_queue)
        self.registry.db.unlock_tables()

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
                for lfile in (all_files - replica.files()):
                    self.rlfsm.subscribe_file(replica.site, lfile)

        self.message = 'Data is injected.'

# exported to __init__.py
export_data = {
    'inject': InjectData,
    'injectsync': InjectDataSync
}
