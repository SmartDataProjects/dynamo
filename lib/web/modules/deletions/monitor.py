from dynamo.web.modules._base import WebModule
from dynamo.web.modules._html import HTMLMixin
from dynamo.fileop.rlfsm import RLFSM

class FileDeletionActivity(WebModule, HTMLMixin):
    """
    Open the file deletion list HTML and let activity.js take care of data loading.
    """

    def __init__(self, config):
        WebModule.__init__(self, config)
        HTMLMixin.__init__(self, 'Deletion Activity', 'deletions/activity.html')

        self.stylesheets = ['/css/deletions/monitor.css']
        self.scripts = ['/js/plotly-jan2018.min.js',
                        '/js/utils.js',
                        '/js/deletions/activity.js']
        
    def run(self, caller, request, inventory):

        if 'graph' in request:
            graph = request['graph']
        else:
            graph = 'volume'

        if 'src_filter' in request:
            src_filter = request['src_filter']
        else:
            src_filter = ''

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
            '","' +  src_filter + \
            '","' +  no_mss + \
            '","' +  period + \
            '","' +  upto + \
            '","' +  exit_code + \
            '"); });'
        return self.form_html()

export_web = {
    'activity': FileDeletionActivity
}
