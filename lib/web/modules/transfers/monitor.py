from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.fileop.rlfsm import RLFSM

class FileTransferList(WebModule, HTMLMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Current file transfers', 'transfers/monitor.html')

        self.stylesheets = ['/css/transfers/monitor.css']
        self.scripts = ['/js/utils.js', '/js/transfers/monitor.js']

    def run(self, caller, request, inventory):
        self.header_script = '$(document).ready(function() { initPage(); });'
        return self.form_html()

export_web = {
    'list': FileTransferList
}
