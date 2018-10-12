from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.fileop.rlfsm import RLFSM

class FileTransferList(WebModule, HTMLMixin):
    """
    Open the file transfer list HTML and let monitor.js take care of data loading.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Current file transfers', 'transfers/monitor.html')

        self.stylesheets = ['/css/transfers/monitor.css']
        self.scripts = ['/js/utils.js', '/js/transfers/monitor.js']

    def run(self, caller, request, inventory):
        self.header_script = '$(document).ready(function() { initPage(); });'
        return self.form_html()

class FileTransferActivity(WebModule, HTMLMixin):
    """
    Open the file transfer list HTML and let activity.js take care of data loading.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Transfer Activity', 'transfers/activity.html')

        self.stylesheets = ['/css/transfers/monitor.css']
        self.scripts = ['/js/plotly-jan2018.min.js',
                        '/js/utils.js',
                        '/js/transfers/activity.js']
        
    def run(self, caller, request, inventory):

        if 'graph' in request:
            graph = request['graph']
        else:
            graph = 'volume'

        if 'entity' in request:
            entity = request['entity']
        else:
            entity = 'dest'

        if 'src_filter' in request:
            src_filter = request['src_filter']
        else:
            src_filter = ''

        if 'dest_filter' in request:
            dest_filter = request['dest_filter']
        else:
            dest_filter = ''

        if 'no_mss' in request:
            no_mss = request['no_mss']
        else:
            no_mss = 't'

        if 'period' in request:
            period = request['period']
        else:
            period = '96h'

        if 'upto' in request:
            upto = request['upto']
        else:
            upto = '0h'

        if 'exit_code' in request:
            exit_code = request['exit_code']
        else:
            exit_code = '0'

        self.header_script = \
            '$(document).ready(function() { initPage("'  \
            + graph + \
            '","' +  entity + \
            '","' +  src_filter + \
            '","' +  dest_filter + \
            '","' +  no_mss + \
            '","' +  period + \
            '","' +  upto + \
            '","' +  exit_code + \
            '"); });'
        return self.form_html()

from dynamo.web.modules.transfers.current import CurrentFileTransfers

class CurrentFileTransferListStatic(WebModule, HTMLMixin):
    """
    Simpler example of cuirrent file transfer listing using direct python HTML formatting.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Current file transfers', 'transfers/monitor_static.html')

        self.stylesheets = ['/css/transfers/monitor.css']

        # Instantiate the JSON producer
        self.current = CurrentFileTransfers(config)

    def run(self, caller, request, inventory):
        data = self.current.run(caller, request, inventory)

        rows = ''
        for transfer in data:
            rows += '<tr>'
            rows += '<td>%d</td>' % transfer['id']
            rows += '<td>%s</td>' % transfer['from']
            rows += '<td>%s</td>' % transfer['to']
            rows += '<td class="lfn">%s</td>' % transfer['lfn']
            rows += '<td>%.2f</td>' % (transfer['size'] * 1.e-9)
            rows += '<td>%s</td>' % transfer['status']
            rows += '<td>%s</td>' % transfer['start']
            rows += '<td>%s</td>' % transfer['finish']
            rows += '</tr>'

        # body_html is already set to the contents of monitor_static.html
        self.body_html = self.body_html.format(_ROWS_ = rows)

        return self.form_html()

class HeldTransferList(WebModule, HTMLMixin):
    """
    Open the file transfer list HTML and let monitor.js take care of data loading.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Suspended file transfers', 'transfers/held.html')

        self.stylesheets = ['/css/transfers/held.css']
        self.scripts = ['/js/utils.js', '/js/transfers/held.js']

    def run(self, caller, request, inventory):
        self.header_script = '$(document).ready(function() { initPage(); });'
        return self.form_html()

from dynamo.web.modules.transfers.history import FileTransferHistory

class HistoryFileTransferListStatic(WebModule, HTMLMixin):
    """
    Simpler example of history file transfer listing using direct python HTML formatting.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Historic file transfers (only 100)', 'transfers/history_list.html')

        self.stylesheets = ['/css/transfers/monitor.css']

        # Instantiate the JSON producer
        self.history = FileTransferHistory(config)

    def run(self, caller, request, inventory):
        data = self.history.run(caller, request, inventory)

        rows = ''
        for transfer in data:
            rows += '<tr>'
            rows += '<td>%s</td>' % transfer['from']
            rows += '<td>%s</td>' % transfer['to']
            rows += '<td class="lfn">%s</td>' % transfer['lfn']
            rows += '<td>%.2f</td>' % (transfer['size'] * 1.e-9)
            rows += '<td>%d </td>' % transfer['exitcode']
            rows += '<td>%s</td>' % transfer['create']
            rows += '<td>%s</td>' % transfer['start']
            rows += '<td>%s</td>' % transfer['finish']
            rows += '<td>%s</td>' % transfer['complete']
            rows += '</tr>'

        # body_html is already set to the contents of monitor_static.html
        self.body_html = self.body_html.format(_ROWS_ = rows)

        return self.form_html()

export_web = {
    'list': FileTransferList,
    'activity': FileTransferActivity,
    'current_list': CurrentFileTransferListStatic,
    'history_list': HistoryFileTransferListStatic,
    'held': HeldTransferList
}
