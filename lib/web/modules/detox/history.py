import os
import fnmatch
import re

from dynamo.web.modules._base import WebModule
from dynamo.web.modules._mysqlhistory import MySQLHistoryMixin
from dynamo.web.modules._filedownload import FileDownloadMixin
from dynamo.web.modules._common import yesno
import dynamo.web.exceptions as exceptions
from dynamo.detox.history import DetoxHistoryBase
from dynamo.dataformat import Configuration

class DetoxHistory(WebModule, MySQLHistoryMixin):
    def __init__(self, config):
        WebModule.__init__(self, config)
        MySQLHistoryMixin.__init__(self, config)

        # The partition that shows up when the page is opened with no arguments
        self.default_partition = config.detox.default_partition
        # List of partitions whose timestamp can go red if the update has not happened for a long while
        self.monitored_partitions = config.detox.monitored_partitions

        self.operation = 'deletion'

        self.partition_id = 0
        self.cycle = 0
        self.policy_version = ''
        self.comment = ''
        self.timestamp = ''

    def from_partition(self, name = ''):
        if not name:
            name = self.default_partition

        try:
            self.partition_id = self.history.query('SELECT `id` FROM `partitions` WHERE `name` = %s', name)[0]
        except IndexError:
            raise exceptions.InvalidRequest('Unknown partition %s' % name)

    def get_cycle(self, cycle):
        sql = 'SELECT `id`, `policy_version`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `cycles`'
        sql += ' WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s AND `id` = %s'
        result = self.history.query(sql, self.partition_id, self.operation, cycle)

        if len(result) == 0:
            return

        self.cycle = result[0][0]
        self.policy_version = result[0][1]
        self.comment = result[0][2]
        self.timestamp = result[0][3]

    def get_latest_cycle(self):
        sql = 'SELECT `id`, `policy_version`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `cycles`'
        sql += ' WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s ORDER BY `id` DESC LIMIT 1'
        result = self.history.query(sql, self.partition_id, self.operation)

        if len(result) == 0:
            # this partition has no run
            return

        self.cycle = result[0][0]
        self.policy_version = result[0][1]
        self.comment = result[0][2]
        self.timestamp = result[0][3]


class DetoxPartitions(DetoxHistory):
    def __init__(self, config):
        DetoxHistory.__init__(self, config)

        self.excluded_partitions = config.detox.history.get('excluded_partitions', [])

    def run(self, caller, request, inventory):
        sql = 'SELECT DISTINCT `partitions`.`id`, `partitions`.`name` FROM `cycles`'
        sql += ' INNER JOIN `partitions` ON `partitions`.`id` = `cycles`.`partition_id`'
        sql += ' WHERE `cycles`.`operation` = %s ORDER BY `partitions`.`id`'

        data = []

        for partition_id, partition in self.history.xquery(sql, self.operation):
            if partition in self.excluded_partitions:
                continue

            if partition == self.default_partition:
                data.insert(0, {'id': partition_id, 'name': partition, 'monitored': (partition in self.monitored_partitions)})
            else:
                data.append({'id': partition_id, 'name': partition, 'monitored': (partition in self.monitored_partitions)})

        return data


class DetoxCycles(DetoxHistory):
    def run(self, caller, request, inventory):
        if 'partition_id' in request:
            self.partition_id = int(request['partition_id'])
        else:
            self.from_partition()

        if ('cycle' in request and request['cycle'] == '0') or ('latest' in request and yesno(request, 'latest')):
            self.get_latest_cycle()
            return [{'cycle': self.cycle, 'partition_id': self.partition_id, 'policy_version': self.policy_version, 'comment': self.comment, 'timestamp': self.timestamp}]

        elif 'cycle' in request:
            cycle = int(request['cycle'])

            self.get_cycle(cycle)

            if self.cycle != cycle:
                self.get_latest_cycle()

            return [{'cycle': self.cycle, 'partition_id': self.partition_id, 'policy_version': self.policy_version, 'comment': self.comment, 'timestamp': self.timestamp}]

        else:
            sql = 'SELECT `id`, `policy_version`, `comment`, UNIX_TIMESTAMP(`time_start`) FROM `cycles`'
            sql += ' WHERE `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = %s ORDER BY `id`'

            data = []
            for cycle, policy_version, comment, timestamp in self.history.xquery(sql, self.partition_id, self.operation):
                data.append({'cycle': cycle, 'partition_id': self.partition_id, 'policy_version': policy_version, 'comment': comment, 'timestamp': timestamp})

            return data


class DetoxHistoryCached(DetoxHistory):
    """
    DetoxHistory with a handle to dynamo.detox.history:DetoxHistoryBase to deal with site and replica cache.
    """

    def __init__(self, config):
        DetoxHistory.__init__(self, config)

        dh_config = Configuration({
            'history_db': self.history.db_name(),
            'cache_db': config.detox.cache_db,
            'snapshots_spool_dir': config.spool_path + '/detox_snapshots',
            'snapshots_archive_dir': config.archive_path + '/detox_snapshots'
        })
        self.detox_history = DetoxHistoryBase(dh_config)
        self.detox_history._mysql = self.history



class DetoxCycleSummary(DetoxHistoryCached):
    def run(self, caller, request, inventory):
        try:
            cycle = int(request['cycle'])
        except KeyError:
            self.get_latest_cycle()
        else:
            self.get_cycle(cycle)

        data = {
            'operation': self.operation,
            'cycle': self.cycle,
            'cycle_policy': self.policy_version,
            'comment': self.comment,
            'cycle_timestamp': self.timestamp,
            'partition': self.partition_id,
            'site_data': []
        }

        sql = 'SELECT `id` FROM `cycles` WHERE `id` < %s AND `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\''
        sql += ' AND `operation` = %s ORDER BY `id` DESC LIMIT 1'
        try:
            data['previous_cycle'] = self.history.query(sql, self.cycle, self.partition_id, self.operation)[0]
        except IndexError:
            data['previous_cycle'] = 0

        sql = 'SELECT `id` FROM `cycles` WHERE `id` > %s AND `partition_id` = %s AND `time_end` NOT LIKE \'0000-00-00 00:00:00\''
        sql += ' AND `operation` = %s ORDER BY `id` ASC LIMIT 1'
        try:
            data['next_cycle'] = self.history.query(sql, self.cycle, self.partition_id, self.operation)[0]
        except IndexError:
            data['next_cycle'] = 0

        ## Now get the site data
        site_data = data['site_data']

        siteinfo = self.detox_history.get_sites(self.cycle, skip_unused = True)
        decisions = self.detox_history.get_deletion_decisions(self.cycle, size_only = True)
        prev_decisions = self.detox_history.get_deletion_decisions(data['previous_cycle'], size_only = True)

        total = {'quota': 0., 'protect': 0., 'keep': 0., 'delete': 0., 'protect_prev': 0., 'keep_prev': 0.}
        
        for sname in sorted(siteinfo.iterkeys()):
            sid, status, quota = siteinfo[sname]

            if status in ('morgue', 'waitroom'):
                status_bit = 0
            else:
                status_bit = 1

            site_data.append({
                'name': sname,
                'quota': quota,
                'status': status_bit,
                'protect': decisions[sname]['protect'],
                'keep': decisions[sname]['keep'],
                'delete': decisions[sname]['delete'],
                'protect_prev': prev_decisions[sname]['protect'],
                'keep_prev': prev_decisions[sname]['keep']
            })

            for key in total.iterkeys():
                total[key] += site_data[-1][key]

        # First element is the total
        total['name'] = 'Total'
        site_data.insert(0, total)

        return data


class DetoxCycleDump(DetoxHistoryCached, FileDownloadMixin):
    def __init__(self, config):
        DetoxHistoryCached.__init__(self, config)

    def run(self, caller, request, inventory):
        try:
            cycle = int(request['cycle'])
        except KeyError:
            self.get_latest_cycle()
        else:
            self.get_cycle(cycle)

        decisions = self.detox_history.get_deletion_decisions(self.cycle, size_only = False, decisions = ['delete'])

        dump = ''
        for site_name, site_decisions in decisions.iteritems():
            for dataset_name, replica_size, decision, condition_id, condition_text in site_decisions:
                dump += '%s\t%s\t%.2f\n' % (site_name, dataset_name, replica_size * 1.e-9)

        return self.export_content(dump, 'deletions_%d.txt' % self.cycle)


class DetoxSiteDetail(DetoxHistoryCached):
    def run(self, caller, request, inventory):
        try:
            cycle = int(request['cycle'])
        except KeyError:
            self.get_latest_cycle()
        else:
            self.get_cycle(cycle)

        try:
            sname = request['site']
        except KeyError:
            raise exceptions.MissingParameter('site')

        data = {'content': {'name': sname, 'datasets': []}, 'conditions': {}}

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


class DetoxDatasetSearch(DetoxHistoryCached):
    def run(self, caller, request, inventory):
        try:
            cycle = int(request['cycle'])
        except KeyError:
            self.get_latest_cycle()
        else:
            self.get_cycle(cycle)

        try:
            pattern_strings = request['datasets']
        except KeyError:
            raise exceptions.MissingParameter('datasets')

        data = {'results': [], 'conditions': {}}
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

            for site_name, site_decisions in decisions.iteritems():
                site_datasets = []
                ma = multi_action[site_name]
    
                for dataset_name, replica_size, decision, condition_id, condition_text in site_decisions:
                    if not match(dataset_name):
                        continue
    
                    if dataset_name in ma:
                        decision += ' *'
    
                    site_datasets.append({'name': dataset_name, 'size': replica_size * 1.e-9, 'decision': decision, 'conditionId': condition_id})
                    if condition_id not in conditions:
                        conditions[condition_id] = condition_text

                if len(site_datasets) != 0:
                    site_data.append({'name': site_name, 'datasets': site_datasets})
    
            data['results'].append({'pattern': pattern, 'site_data': site_data})
                        
        return data

export_data = {
    'partitions': DetoxPartitions,
    'cycles': DetoxCycles,
    'summary': DetoxCycleSummary,
    'sitedetail': DetoxSiteDetail,
    'datasets': DetoxDatasetSearch,
    'dump': DetoxCycleDump
}

def test(cls):
    def init(config):
        instance = cls(config)
        instance.operation = 'deletion_test'
        return instance

    return init

for key, cls in export_data.items():
    export_data['test/' + key] = test(cls)
