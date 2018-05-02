from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlregistry import MySQLRegistryMixin
from dynamo.web.modules._mysqlhistory import MySQLHistoryMixin

class Example(WebModule, MySQLRegistryMixin, MySQLHistoryMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLRegistryMixin.__init__(self, config)
        MySQLHistoryMixin.__init__(self, config)

    def run(self, caller, request, inventory):
        reg_sites = self.registry.query('SELECT `site` FROM `transfer_queue`')
        hist_sites = self.history.query('SELECT `site` FROM `transfer_queue`')

        return []

## Define the mapping from PATH_INFO (https://server/module/PATH_INFO) to class
export_data = {
    'example': Example
}

# This exports dictionary is loaded in __init__.py of this directory
