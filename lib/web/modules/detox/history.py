import os
import fnmatch
import re

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._filedownload import FileDownloadMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions
from dynamo.detox.history import DetoxHistoryBase
from dynamo.dataformat import Configuration

class WebDetoxHistory(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.detox_history = DetoxHistoryBase()

        # The partition that shows up when the page is opened with no arguments
        self.default_partition = config.detox.default_partition
        # List of partitions whose timestamp can go red if the update has not happened for a long while
        self.monitored_partitions = config.detox.monitored_partitions

        self.operation = 'deletion'

        self.partition_id = 0
        self.cycle = 0
        self.comment = ''
        self.timestamp = ''

    def from_partition(self, name = ''):
        if not name:
            name = self.default_partition

        try:
            self.partition_id = self.detox_history.db.query('SELECT `id` FROM `partitions` WHERE `name` = %s', name)[0]
        except IndexError:
            raise exceptions.InvalidRequest('Unknown partition %s' % name)

    def get_cycle(self, cycle):
        sql = 'SELECT `id`, `partition_id`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `deletion_cycles`'
        sql += ' WHERE `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s AND `id` = %s'
        result = self.detox_history.db.query(sql, self.operation, cycle)

        if len(result) == 0:
            return

        if self.partition_id != 0 and result[0][1] != self.partition_id:
            return

        self.cycle = result[0][0]
        self.partition_id = result[0][1]
        self.comment = result[0][2]
        self.timestamp = result[0][3]

    def get_latest_cycle(self):
        if self.partition_id == 0:
            self.from_partition()

        sql = 'SELECT `id`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `deletion_cycles`'
        sql += ' WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s ORDER BY `id` DESC LIMIT 1'
        result = self.detox_history.db.query(sql, self.partition_id, self.operation)

        if len(result) == 0:
            # this partition has no run
            return

        self.cycle = result[0][0]
        self.comment = result[0][1]
        self.timestamp = result[0][2]

    def get_partition_and_cycle(self, request, default_latest = True):
        if 'partition' in request:
            self.from_partition(request['partition'])

        elif 'partition_id' in request:
            try:
                self.partition_id = int(request['partition_id'])
            except ValueError:
                raise exceptions.IllFormedRequest('partition_id', request['partition_id'])

        # partition_id can still be 0 here

        requested_cycle = None

        if 'cycle' in request:
            try:
                requested_cycle = int(request['cycle'])
            except ValueError:
                raise exceptions.IllFormedRequest('cycle', request['cycle'])

            if requested_cycle != 0: # case 0 considered later
                self.get_cycle(requested_cycle)
                return requested_cycle

        elif 'latest' in request and yesno(request, 'latest'):
            requested_cycle = 0

        # we either don't have a cycle request or are requested the latest cycle

        if self.partition_id == 0:
            self.from_partition()

        if requested_cycle is not None or default_latest:
            # requested_cycle must be 0 if not None
            self.get_latest_cycle()

        return requested_cycle


class DetoxPartitions(WebDetoxHistory):
    def __init__(self, config):
        WebDetoxHistory.__init__(self, config)

        self.excluded_partitions = config.detox.get('excluded_partitions', [])

    def run(self, caller, request, inventory):
        sql = 'SELECT DISTINCT p.`id`, p.`name` FROM `deletion_cycles` AS c'
        sql += ' INNER JOIN `partitions` AS p ON p.`id` = c.`partition_id`'
        sql += ' WHERE c.`operation` = %s'
        if 'cycle' in request:
            try:
                sql += ' AND c.`id` = %d' % int(request['cycle'])
            except ValueError:
                raise exceptions.IllFormedRequest('cycle', request['cycle'])
        sql += ' ORDER BY p.`id`'

        data = []

        for partition_id, partition in self.detox_history.db.xquery(sql, self.operation):
            if partition in self.excluded_partitions:
                continue

            if 'partition' in request:
                if not fnmatch.fnmatch(partition, request['partition']):
                    continue

            if partition == self.default_partition:
                # default always comes first
                data.insert(0, {'id': partition_id, 'name': partition, 'monitored': (partition in self.monitored_partitions)})
            else:
                data.append({'id': partition_id, 'name': partition, 'monitored': (partition in self.monitored_partitions)})

        return data


class DetoxCycles(WebDetoxHistory):
    def run(self, caller, request, inventory):
        requested_cycle = self.get_partition_and_cycle(request, default_latest = False)

        if requested_cycle is not None:
            if self.cycle == 0:
                return []
            else:
                return [{'cycle': self.cycle, 'partition_id': self.partition_id, 'comment': self.comment, 'timestamp': self.timestamp}]

        else:
            sql = 'SELECT `id`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `deletion_cycles`'
            sql += ' WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s ORDER BY `id`'

            data = []
            for cycle, comment, timestamp in self.detox_history.db.xquery(sql, self.partition_id, self.operation):
                data.append({'cycle': cycle, 'partition_id': self.partition_id, 'comment': comment, 'timestamp': timestamp})

            return data


class DetoxCycleSummary(WebDetoxHistory):
    def run(self, caller, request, inventory):
        self.get_partition_and_cycle(request)

        data = {
            'operation': self.operation,
            'cycle': self.cycle,
            'comment': self.comment,
            'cycle_timestamp': self.timestamp,
            'partition': self.partition_id,
            'site_data': []
        }

        sql = 'SELECT `id` FROM `deletion_cycles` WHERE `id` < %s AND `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\''
        sql += ' AND `operation` = %s ORDER BY `id` DESC LIMIT 1'
        try:
            data['previous_cycle'] = self.detox_history.db.query(sql, self.cycle, self.partition_id, self.operation)[0]
        except IndexError:
            data['previous_cycle'] = 0

        sql = 'SELECT `id` FROM `deletion_cycles` WHERE `id` > %s AND `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\''
        sql += ' AND `operation` = %s ORDER BY `id` ASC LIMIT 1'
        try:
            data['next_cycle'] = self.detox_history.db.query(sql, self.cycle, self.partition_id, self.operation)[0]
        except IndexError:
            data['next_cycle'] = 0

        ## Now get the site data
        site_data = data['site_data']

        siteinfo = self.detox_history.get_sites(self.cycle, skip_unused = True)
        decisions = self.detox_history.get_deletion_decisions(self.cycle, size_only = True)
        if data['previous_cycle'] != 0:
            prev_decisions = self.detox_history.get_deletion_decisions(data['previous_cycle'], size_only = True)
        else:
            prev_decisions = {}

        total = {'quota': 0., 'protect': 0., 'keep': 0., 'delete': 0., 'protect_prev': 0., 'keep_prev': 0.}
        
        for sname in sorted(siteinfo.iterkeys()):
            status, quota = siteinfo[sname]

            if status in ('morgue', 'waitroom'):
                status_bit = 0
            else:
                status_bit = 1

            decision = decisions[sname]
            try:
                prev_decision = prev_decisions[sname]
            except KeyError:
                prev_decision = (0., 0., 0.)

            site_data.append({
                'name': sname,
                'quota': quota,
                'status': status_bit,
                'protect': decision[0],
                'keep': decision[2],
                'delete': decision[1],
                'protect_prev': prev_decision[0],
                'keep_prev': prev_decision[2]
            })

            for key in total.iterkeys():
                total[key] += site_data[-1][key]

        # First element is the total
        total['name'] = 'Total'
        site_data.insert(0, total)

        return data


class DetoxCycleDump(WebDetoxHistory, FileDownloadMixin):
    def __init__(self, config):
        WebDetoxHistory.__init__(self, config)

    def run(self, caller, request, inventory):
        self.get_partition_and_cycle(request)

        decisions = self.detox_history.get_deletion_decisions(self.cycle, size_only = False, decisions = ['delete'])

        dump = ''
        for site_name, site_decisions in decisions.iteritems():
            for dataset_name, replica_size, decision, condition_id, condition_text in site_decisions:
                dump += '%s\t%s\t%.2f\n' % (site_name, dataset_name, replica_size * 1.e-9)

        return self.export_content(dump, 'deletions_%d.txt' % self.cycle)


class DetoxCyclePolicy(WebDetoxHistory):
    def __init__(self, config):
        WebDetoxHistory.__init__(self, config)

        self.content_type = 'text/plain'

    def run(self, caller, request, inventory):
        if 'cycle' not in request:
            raise exceptions.MissingParameter('cycle')

        try:
            cycle = int(request['cycle'])
        except ValueError:
            raise exceptions.IllFormedRequest('cycle', request['cycle'])

        sql = 'SELECT `text` FROM `deletion_policies` WHERE `id` = (SELECT `policy_id` FROM `deletion_cycles` WHERE `id` = %s)'
        try:
            text = self.detox_history.db.query(sql, cycle)[0]
        except:
            raise exceptions.InvalidRequest('Could not find policy text for cycle %d' % cycle)

        return text


class DetoxSiteDetail(WebDetoxHistory):
    def run(self, caller, request, inventory):
        self.get_partition_and_cycle(request)

        try:
            sname = request['site']
        except KeyError:
            raise exceptions.MissingParameter('site')

        data = {'content': {'name': sname, 'datasets': []}, 'conditions': {0: 'No policy match'}}

        decisions = self.detox_history.get_site_deletion_decisions(self.cycle, sname)

        multi_action = set()
        _dataset_name = ''
        for dataset_name in sorted(d[0] for d in decisions):
            if dataset_name == _dataset_name:
                multi_action.add(dataset_name)

            _dataset_name = dataset_name

        dataset_list = data['content']['datasets']
        conditions = data['conditions']

        for dataset_name, replica_size, decision, condition_id, condition_text in decisions:
            if dataset_name in multi_action:
                decision += ' *'

            dataset_list.append({'name': dataset_name, 'size': replica_size * 1.e-9, 'decision': decision, 'condition_id': condition_id})
            if condition_id not in conditions:
                conditions[condition_id] = condition_text

        return data


class DetoxDatasetSearch(WebDetoxHistory):
    def run(self, caller, request, inventory):
        self.get_partition_and_cycle(request)

        try:
            pattern_strings = request['datasets']
        except KeyError:
            raise exceptions.MissingParameter('datasets')

        data = {'results': [], 'conditions': {0: 'No policy match'}}
        conditions = data['conditions']

        decisions = self.detox_history.get_deletion_decisions(self.cycle, size_only = False)

        multi_action = {}
        for site_name, site_decisions in decisions.iteritems():
            ma = multi_action[site_name] = set()
            _dataset_name = ''
            for dataset_name in sorted(d[0] for d in site_decisions):
                if dataset_name == _dataset_name:
                    ma.add(dataset_name)
    
                _dataset_name = dataset_name

        for pattern in pattern_strings:
            if '*' not in pattern and '?' not in pattern:
                match = lambda d: d == pattern
            else:
                regex = re.compile(fnmatch.translate(pattern))
                match = lambda d: regex.match(d)

            site_data = []
            protect_total = 0.
            keep_total = 0.
            delete_total = 0.

            for site_name, site_decisions in decisions.iteritems():
                site_datasets = []
                ma = multi_action[site_name]
    
                for dataset_name, replica_size, decision, condition_id, condition_text in site_decisions:
                    if not match(dataset_name):
                        continue

                    if decision == 'protect':
                        protect_total += replica_size * 1.e-9
                    elif decision == 'keep':
                        keep_total += replica_size * 1.e-9
                    elif decision == 'delete':
                        delete_total += replica_size * 1.e-9
    
                    if dataset_name in ma:
                        decision += ' *'
    
                    site_datasets.append({'name': dataset_name, 'size': replica_size * 1.e-9, 'decision': decision, 'condition_id': condition_id})
                    if condition_id not in conditions:
                        conditions[condition_id] = condition_text

                if len(site_datasets) != 0:
                    site_data.append({'name': site_name, 'datasets': site_datasets})

            site_data.append({'name': 'Total', 'protect': protect_total, 'keep': keep_total, 'delete': delete_total})
    
            data['results'].append({'pattern': pattern, 'site_data': site_data})

        return data

export_data = {
    'partitions': DetoxPartitions,
    'cycles': DetoxCycles,
    'summary': DetoxCycleSummary,
    'sitedetail': DetoxSiteDetail,
    'datasets': DetoxDatasetSearch,
    'dump': DetoxCycleDump,
    'policy': DetoxCyclePolicy
}

def test(cls):
    def init(config):
        instance = cls(config)
        instance.operation = 'deletion_test'
        return instance

    return init

for key, cls in export_data.items():
    export_data['test/' + key] = test(cls)
