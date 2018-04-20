import fnmatch
import re

from dynamo.dataformat import Dataset

def list_datasets(request, inventory, registry, master_server):
    """
    An example web routine. This function is called by dynamo.web.server when there is a call to https://address/get/datasets.
    URL to function mapping is defined in the __init__.py file in this directory.
    When this function is called, the requesting user has been authenticated and all basic tasks are done. The function only needs
    to return something that can be JSONized and returned to the client.

    @param request        A dictionary containing the HTTP request (https://address/get/datasets?dataset=/A/B/C -> {'dataset': '/A/B/C'})
    @param inventory      Dynamo inventory.
    @param registry       MySQL object connected to the registry DB.
    @param master_server  A MasterServer (dynamo.core.components.master) object (gives access to user authorization etc.)

    @return A list containing information of datasets.
    """

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
