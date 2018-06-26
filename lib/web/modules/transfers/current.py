import time

from dynamo.web.modules._base import WebModule
from dynamo.fileop.rlfsm import RLFSM

class CurrentFileTransfers(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.rlfsm = RLFSM()
        self.rlfsm.set_read_only(True)

    def run(self, caller, request, inventory):
        sql = 'SELECT q.`id`, ss.`name`, sd.`name`, f.`name` FROM `transfer_tasks` AS q'
        sql += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        sql += ' INNER JOIN `sites` AS ss ON ss.`id` = q.`source_id`'
        sql += ' INNER JOIN `sites` AS sd ON sd.`id` = u.`site_id`'
        sql += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        sql += ' ORDER BY q.`id`'

        current_tasks = self.rlfsm.db.query(sql)

        transfers = self.rlfsm.transfer_query.get_transfer_status()
        transfers_map = dict((t[0], t[1:]) for t in transfers)
        
        data = []
        for task_id, source, destination, lfn in transfers:
            try:
                transfer = transfers_map[task_id]
            except KeyError:
                status = 'unknown'
                start = ''
                finish = ''
            else:
                status = transfer[1]
                start = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(transfer[3]))
                finish = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(transfer[4]))

            data.append({'id': task_id, 'from': source, 'to': destination, 'lfn': lfn, 'status': status, 'start': start, 'finish': finish})

        return data

export_data = {
    'current': CurrentFileTransfers
}
