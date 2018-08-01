import time

from dynamo.web.modules._base import WebModule
from dynamo.fileop.rlfsm import RLFSM
from dynamo.fileop.base import FileQuery

class HeldFileTransferSummary(WebModule):
    """
    Return a list of held transfers by destination and reason
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

        self.rlfsm = RLFSM()
        self.rlfsm.set_read_only(True)

    def run(self, caller, request, inventory):
        get_all = 'SELECT s.`name`, u.`hold_reason` FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        get_all += ' WHERE u.`delete` = 0 AND u.`status` = \'held\''

        reasons = ['no_source', 'all_failed', 'site_unavailable', 'unknown']

        by_dest = {}
        for site, reason in self.rlfsm.db.query(get_all):
            if site not in by_dest:
                by_dest[site] = dict((r, 0) for r in reasons)

            if reason is None:
                reason = 'unknown'

            by_dest[site][reason] += 1

        data = []
        for site, counts in by_dest.iteritems():
            data.append({'site': site, 'counts': counts})

        return data

class HeldFileTransferDetail(WebModule):
    """
    Return details of held transfers
    """

    def __init__(self, config):
        WebModule.__init__(self, config)

        self.rlfsm = RLFSM()
        self.rlfsm.set_read_only(True)

    def run(self, caller, request, inventory):
        get_all = 'SELECT u.`id`, f.`name`, s.`name`, u.`hold_reason`, l.`exitcode`, ss.`name` FROM `file_subscriptions` AS u'
        get_all += ' INNER JOIN `files` AS f ON f.`id` = u.`file_id`'
        get_all += ' INNER JOIN `sites` AS s ON s.`id` = u.`site_id`'
        get_all += ' LEFT JOIN `failed_transfers` AS l ON l.`subscription_id` = u.`id`'
        get_all += ' LEFT JOIN `sites` AS ss ON ss.`id` = l.`source_id`'
        get_all += ' WHERE u.`delete` = 0 AND u.`status` = \'held\''

        args = []
        if 'site' in request:
            get_all += ' AND s.`name` = %s'
            args.append(request['site'])

        get_all += ' ORDER BY u.`id`'

        data = []
        _subid = 0
        for subid, filename, destname, reason, exitcode, sourcename in self.rlfsm.db.query(get_all, *args):
            if reason is None:
                reason = 'unknown'

            if subid != _subid:
                _subid = subid
                data.append({'id': subid, 'file': filename, 'destination': destname, 'reason': reason})
                datum = data[-1]

                if reason == 'all_failed':
                    datum['attempts'] = []

            if reason == 'all_failed':
                datum['attempts'].append({'source': sourcename, 'exitcode': exitcode})

        return data


export_data = {
    'held/summary': HeldFileTransferSummary,
    'held/detail': HeldFileTransferDetail
}
