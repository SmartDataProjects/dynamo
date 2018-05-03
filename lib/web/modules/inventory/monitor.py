import json

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions

class DatasetStats(WebModule, HTMLMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Dynamo dataset statistics', config.inventory.monitor.body_html)

        self.stylesheets = ['css/inventory.css']
        self.scripts = ['js/utils.js', 'js/inventory.js']
        self.header_script = '$(document).ready(function() { initPage(\'{DATA_TYPE}\', \'{CATEGORIES}\', {CONSTRAINTS}); });'

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults
        repl = {}

        try:
            repl['DATA_TYPE'] = request.getvalue('dataType').strip()
        except:
            repl['DATA_TYPE'] = 'size'

        try:
            repl['CATEGORIES'] = request.getvalue('categories').strip()
        except:
            repl['CATEGORIES'] = 'campaigns'

        constraints = {}
        for key in ['campaign', 'dataTier', 'dataset', 'site']:
            try:
                constraints[key] = request.getvalue(key).strip()
            except:
                pass

        group = request.getvalue('group')
        if type(group) is list:
            constraints['group'] = group
        elif type(group) is str:
            constraints['group'] = group.strip().split(',')

        if len(constraints) == 0:
            constraints['group'] = ['AnalysisOps']

        repl['CONSTRAINTS'] = json.dumps(constraints)

        if yesno(request, 'physical'):
            repl['PHYSICAL_CHECKED'] = 'checked="checked"'
            repl['PROJECTED_CHECKED'] = ''
        else:
            repl['PHYSICAL_CHECKED'] = ''
            repl['PROJECTED_CHECKED'] = 'checked="checked"'

        return self.form_html(repl)


export_web = {
    'datasets': DatasetStats
}
