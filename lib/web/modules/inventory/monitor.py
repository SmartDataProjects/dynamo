import json

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions

class DatasetStats(WebModule, HTMLMixin):
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


export_web = {
    'datasets': DatasetStats
}
