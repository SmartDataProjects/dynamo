from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions

class DetoxMonitor(WebModule, HTMLMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Detox deletion results', 'detox/monitor.html')

        self.stylesheets = ['/css/detox/monitor.css']
        self.scripts = ['/js/utils.js', '/js/detox/monitor.js']
        
        with open(HTMLMixin.contents_path + '/html/detox/monitor_titleblock.html') as source:
            self.titleblock = source.read()

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults



        # HTML formatting

        self.header_script = '$(document).ready(function() { initPage(${CYCLE_NUMBER}, ${PARTITION}); });'

        repl = {}

 
        if yesno(request, 'physical'):
            repl['PHYSICAL_CHECKED'] = 'checked="checked"'
            repl['PROJECTED_CHECKED'] = ''
        else:
            repl['PHYSICAL_CHECKED'] = ''
            repl['PROJECTED_CHECKED'] = 'checked="checked"'

        return self.form_html(repl)

export_web = {
    '': DetoxMonitor
}
