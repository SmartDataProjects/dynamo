import time

from dynamo.web.modules._base import WebModule
from dynamo.history.history import HistoryDatabase

class FileTransferHistory(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.history = HistoryDatabase()

    def run(self, caller, request, inventory):
        sql = 'SELECT ss.`name`, sd.`name`, f.`name`, f.`size`, t.`exitcode`,'
        sql += ' UNIX_TIMESTAMP(t.`created`), UNIX_TIMESTAMP(t.`started`), UNIX_TIMESTAMP(t.`finished`), UNIX_TIMESTAMP(t.`completed`)'
        sql += ' FROM `file_transfers` AS t'
        sql += ' INNER JOIN `files` AS f ON f.`id` = t.`file_id`'
        sql += ' INNER JOIN `sites` AS ss ON ss.`id` = t.`source_id`'
        sql += ' INNER JOIN `sites` AS sd ON sd.`id` = t.`destination_id`'
        
        ## USING A HARD LIMIT FOR NOW - SHOULD CONTROL USING THE request DICTIONARY
        sql += ' LIMIT 100'
        
        data = []
        for source, destination, filename, size, exitcode, created, started, finished, completed in self.history.db.xquery(sql):
            created = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(created))
            started = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started))
            finished = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(finished))
            completed = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(completed))

            data.append({
                'from': source,
                'to': destination,
                'lfn': filename,
                'size': size,
                'exitcode': exitcode,
                'create': created,
                'start': started,
                'finish': finished,
                'complete': completed
            })

        return data

export_data = {
    'history': FileTransferHistory
}
