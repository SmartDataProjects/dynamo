import sys
import time

def make_dataset_data(datasets, blocks = None, files = None):
    if blocks is not None and len(datasets) != 1:
        raise RuntimeError('--block is only available when one dataset is given')

    if files is not None and (blocks is None or len(blocks) != 1):
        raise RuntimeError('--file is only available when one block is given')

    data = []

    for dentry in datasets:
        specs = dentry.split(',')

        name = specs[0]
        status = 'unknown'
        data_type = 'unknown'
        try:
            status = specs[1]
            data_type = specs[2]
        except IndexError:
            pass

        data.append({'name': name, 'status': status, 'data_type': data_type})

        if blocks is None:
            continue

        block_data = data[-1]['blocks'] = []

        for block_name in blocks:
            block_data.append({'name': block_name})

            if files is None:
                continue

            file_data = block_data[-1]['files'] = []

            for fentry in files:
                specs = fentry.split(',')

                name = specs[0]
                try:
                    size = int(specs[1])
                except IndexError:
                    raise RuntimeError('Size missing for file %s' % name)
                file_data.append({'name': name, 'size': size})
                if len(specs) > 2:
                    file_data[-1]['site'] = specs[2]
                            
    return data

def make_site_data(sites):
    data = []

    for sentry in sites:
        specs = sentry.split(',')

        name = specs[0]
        host = ''
        storage_type = 'unknown'
        backend = ''
        status = 'unknown'
        try:
            host = specs[1]
            storage_type = specs[2]
            backend = specs[3]
            status = specs[4]
        except IndexError:
            pass

        data.append({'name': name, 'host': host, 'storage_type': storage_type, 'backend': backend, 'status': status})

    return data

def make_group_data(groups):
    data = []

    for gentry in groups:
        specs = gentry.split(',')
        
        name = specs[0]
        olevel = 'block'
        try:
            olevel = specs[1]
        except IndexError:
            pass

        data.append({'name': name, 'olevel': olevel})

    return data

def make_dataset_replica_data(dataset_replicas, block_replicas = None, block_replica_files = None):
    if block_replicas is not None and len(dataset_replicas) != 1:
        raise RuntimeError('--block-replica is only available when one dataset replica is given')

    if block_replica_files is not None and (block_replicas is None or len(block_replicas) != 1):
        raise RuntimeError('--block-replica-file is only available when one block replica is given')

    data = []

    for dentry in dataset_replicas:
        specs = dentry.split(',')
        site, _, dataset = specs[0].partition(':')

        if not dataset:
            raise RuntimeError('Invalid dataset replica spec %s' % specs[0])

        data.append({'site': site, 'dataset': dataset})
        if len(specs) > 1:
            data[-1]['growing'] = True
            data[-1]['group'] = specs[1]

        if block_replicas is None:
            continue

        brep_data = data[-1]['blockreplicas'] = []

        for bentry in block_replicas:
            specs = bentry.split(',')

            block_name = specs[0]
            brep_data.append({'block': block_name})
            if len(specs) > 1:
                brep_data[-1]['group'] = specs[1]

            if block_replica_files is None:
                continue

            file_data = brep_data[-1]['files'] = []

            for file_name in block_replica_files:
                file_data.append(file_name)

    return data
