import fnmatch
import re
import json

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions
from dynamo.dataformat import Dataset

class ListDatasets(WebModule):
    """
    Simple dataset listing.
    """

    def run(self, caller, request, inventory):
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


class DatasetStats(WebModule, HTMLMixin):
    """
    The original inventory monitor showing various dataset replica statistics.
    """

    def __init__(self, config):
        WebModule.__init__(self, config) 
        HTMLMixin.__init__(self, 'Dynamo dataset statistics', config.inventory.monitor.body_html)

        self.stylesheets = ['/css/inventory/monitor.css']
        self.scripts = ['/js/utils.js', '/js/inventory/monitor.js']

        self.default_constraints = config.inventory.monitor.default_constraints

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults
        try:
            data_type = request['dataType'].strip()
        except:
            data_type = 'size'

        try:
            categories = request['categories'].strip()
        except:
            categories = 'campaigns'

        constraints = {}
        for key in ['campaign', 'dataTier', 'dataset', 'site']:
            try:
                constraints[key] = request[key].strip()
            except:
                pass
                
        try:
            group = request['group']
        except KeyError:
            pass
        else:
            if type(group) is list:
                constraints['group'] = group
            elif type(group) is str:
                constraints['group'] = group.strip().split(',')

        if len(constraints) == 0:
            constraints = self.default_constraints

        self.header_script = '$(document).ready(function() { initPage(\'%s\', \'%s\', %s); });' % (data_type, categories, json.dumps(constraints))

        repl = {}

        if yesno(request, 'physical', True):
            repl['PHYSICAL_CHECKED'] = ' checked="checked"'
            repl['PROJECTED_CHECKED'] = ''
        else:
            repl['PHYSICAL_CHECKED'] = ''
            repl['PROJECTED_CHECKED'] = ' checked="checked"'

        return self.form_html(repl)



# exported to __init__.py
export_data = {
    'datasets': ListDatasets
}

export_web = {
    'datasets': DatasetStats
}
