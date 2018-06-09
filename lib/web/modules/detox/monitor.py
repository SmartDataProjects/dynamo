from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._mysqlhistory import MySQLHistoryMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions

class DetoxMonitor(WebModule, HTMLMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLHistoryMixin.__init__(self, config)
        HTMLMixin.__init__(self, 'Detox deletion results', 'detox/monitor.html')

        self.stylesheets = ['/css/detox/monitor.css']
        self.scripts = ['/js/utils.js', '/js/detox/monitor.js']
        
        with open(HTMLMixin.contents_path + '/html/detox/monitor_titleblock.html') as source:
            self.titleblock = source.read()

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults
        if 'cycle' in request:
            cycle = int(request['cycle'])
        else:
            cycle = 0

        if 'partition' in request:
            try:
                partition_id = self.history.query('SELECT `id` FROM `partitions` WHERE `name` = %s', request['partition'])[0]
            except IndexError:
                partition_id = 0
        else:
            partition_id = 0

        if 'partition_id' in request:
            partition_id = request['partition_id']

        # HTML formatting

        self.header_script = '$(document).ready(function() { initPage(%d, %d); });' % (cycle, partition_id)

        return self.form_html()

export_web = {
    '': DetoxMonitor
}
