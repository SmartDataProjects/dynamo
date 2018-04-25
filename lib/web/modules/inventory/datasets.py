import fnmatch
import re

from dynamo.dataformat import Dataset

class ListDatasets(object):
    def __init__(self, config, caller):
        pass

    def run(self, request, inventory):
        datasets = []
    
        # collect information from the inventory and registry according to the requests
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
    
        # return any JSONizable python object (maybe should be limited to a list)
        return response

# exported to __init__.py
exports = {'datasets': ListDatasets}
