from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions
from dynamo.history.history import HistoryDatabase

class DetoxMonitor(WebModule, HTMLMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Detox deletion results', 'detox/monitor.html')

        self.history = HistoryDatabase()

        self.stylesheets = ['/css/detox/monitor.css']
        self.scripts = ['/js/utils.js', '/js/detox/monitor.js']
        
        with open(HTMLMixin.contents_path + '/html/detox/monitor_titleblock.html') as source:
            self.titleblock = source.read()

        self.default_partition = config.detox.default_partition
        self.test_cycle = False

    def run(self, caller, request, inventory):
        # Parse GET and POST requests and set the defaults
        if 'cycle' in request:
            cycle = int(request['cycle'])
        else:
            cycle = 0

        partition_id = 0
        if 'partition' in request:
            try:
                partition_id = self.history.db.query('SELECT `id` FROM `partitions` WHERE `name` = %s', request['partition'])[0]
            except IndexError:
                pass

        if 'partition_id' in request:
            partition_id = int(request['partition_id'])

        if partition_id == 0:
            partition_id = self.history.db.query('SELECT `id` FROM `partitions` WHERE `name` = %s', self.default_partition)[0]

        # HTML formatting

        if self.test_cycle:
            set_detox_path = 'detoxPath = dataPath + \'/detox/test\'; '
        else:
            set_detox_path = ''

        self.header_script = '$(document).ready(function() { %sinitPage(%d, %d); });' % (set_detox_path, cycle, partition_id)

        return self.form_html()

def DetoxTestMonitor(config):
    instance = DetoxMonitor(config)
    instance.test_cycle = True
    return instance

export_web = {
    '': DetoxMonitor,
    'test': DetoxTestMonitor
}
