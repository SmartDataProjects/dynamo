import fnmatch
import re

from dynamo.dataformat import Dataset

def list_datasets(inventory, master_server, request):
    datasets = []

    if 'dataset' in request:
        match_name = request['dataset']
        if '*' in match_name:
            pattern = re.compile(fnmatch.translate(match_name))
            for name in inventory.datasets.iterkeys():
                if pattern.match(name):
                    datasets.append(inventory.datasets[name])
            
        else:
            try:
                datasets.append(inventory.datasets[match_name])
            except KeyError:
                pass

    response = []
    for dataset in datasets:
        response.append({'name': dataset.name, 'size': dataset.size, 'num_files': dataset.num_files,
            'status': Dataset.status_name(dataset.status), 'type': Dataset.data_type_name(dataset.data_type)})

    return response
