import time

from dynamo.web.modules._base import WebModule
from dynamo.fileop.rlfsm import RLFSM
from dynamo.fileop.base import FileQuery

class CurrentFileTransfers(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.rlfsm = RLFSM()
        self.rlfsm.set_read_only(True)

    def run(self, caller, request, inventory):
        sql = 'SELECT q.`id`, q.`batch_id`, ss.`name`, sd.`name`, f.`name`, f.`size` FROM `transfer_tasks` AS q'
        sql += ' INNER JOIN `file_subscriptions` AS u ON u.`id` = q.`subscription_id`'
        sql += ' INNER JOIN `sites` AS ss ON ss.`id` = q.`source_id`'
        sql += ' INNER JOIN `sites` AS sd ON sd.`id` = u.`site_id`'
        sql += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        sql += ' ORDER BY q.`id`'

        current_tasks = self.rlfsm.db.query(sql)

        batch_ids = set(s[1] for s in current_tasks)

        transfers = []
        for batch_id in batch_ids:
            transfers.extend(self.rlfsm.transfer_query.get_transfer_status(batch_id))

        transfers_map = dict((t[0], t[1:]) for t in transfers)
        
        data = []
        for task_id, batch_id, source, destination, lfn, size in current_tasks:
            try:
                transfer = transfers_map[task_id]
            except KeyError:
                status = 'unknown'
                start = ''
                finish = ''
            else:
                status = FileQuery.status_name(transfer[0])
                if transfer[2] is None:
                    start = ''
                else:
                    start = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(transfer[2]))
                if transfer[3] is None:
                    finish = ''
                else:
                    finish = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(transfer[3]))

            data.append({
                    'id': task_id,
                    'from': source,
                    'to': destination,
                    'lfn': lfn,
                    'size': size,
                    'status': status,
                    'start': start,
                    'finish': finish})

        return data

export_data = {
    'current': CurrentFileTransfers
}
