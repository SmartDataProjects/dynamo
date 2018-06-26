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

from dynamo.web.modules.transfers.current import CurrentFileTransfers

class FileTransferListStatic(WebModule, HTMLMixin):
    """
    Simpler example of file transfer listing using direct python HTML formatting.
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


export_web = {
#    'list': FileTransferList
    'list': FileTransferListStatic
}
