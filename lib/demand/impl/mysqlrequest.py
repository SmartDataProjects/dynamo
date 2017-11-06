from common.configuration import common_config
from common.interface.mysql import MySQL

class MySQLDatasetRequestStore(object):
    """
    A persistency class for storing dataset request weight information.
    """

    def __init__(self, config):
        db_params = dict(common_config.mysql)
        if 'db_params' in config:
            db_params.update(config['db_params'])

        self._mysql = MySQL(**db_params)

    def load_dataset_requests(self, datasets):
        """
        @param datasets  List of datasets
        @returns (last update unix timestamp, {dataset: {job_id: (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued)}})
        """

        logger.debug('load_dataset_requests()')

        id_dataset_map = {}
        self._mysql.make_map('datasets', datasets, id_object_map = id_dataset_map)

        # pick up requests that are less than 1 year old
        # old requests will be removed automatically next time the access information is saved from memory
        sql = 'SELECT `dataset_id`, `id`, UNIX_TIMESTAMP(`queue_time`), UNIX_TIMESTAMP(`completion_time`), `nodes_total`, `nodes_done`, `nodes_failed`, `nodes_queued` FROM `dataset_requests`'
        sql += ' WHERE `queue_time` > DATE_SUB(NOW(), INTERVAL 1 YEAR) ORDER BY `dataset_id`, `queue_time`'

        num_records = 0

        requests = {}

        # little speedup by not repeating lookups for the same dataset
        current_dataset_id = 0
        for dataset_id, job_id, queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued in self._mysql.xquery(sql):
            num_records += 1

            if dataset_id != current_dataset_id:
                try:
                    dataset = id_dataset_map[dataset_id]
                except KeyError:
                    continue

                current_dataset_id = dataset_id
                requests[dataset] = {}

            requests[dataset][job_id] = (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued)

        last_update = self._mysql.query('SELECT UNIX_TIMESTAMP(`dataset_requests_last_update`) FROM `system`')[0]

        logger.info('Loaded %d dataset request data. Last update at %s UTC', num_records, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(last_update)))

        return (last_update, requests)

    def save_dataset_requests(self, request_list):
        """
        Write information in memory into persistent storage.

        @param request_list  Data on updated requests. Same format as load_dataset_request return value [1]
        """

        datasets = request_list.keys()

        dataset_id_map = {}
        self._mysql.make_map('datasets', datasets, object_id_map = dataset_id_map)

        fields = ('id', 'dataset_id', 'queue_time', 'completion_time', 'nodes_total', 'nodes_done', 'nodes_failed', 'nodes_queued')

        data = []
        for dataset, dataset_request_list in request_list.items():
            dataset_id = dataset_id_map[dataset]

            for job_id, (queue_time, completion_time, nodes_total, nodes_done, nodes_failed, nodes_queued) in dataset_request_list.items():
                data.append((
                    job_id,
                    dataset_id,
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(queue_time)),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(completion_time)) if completion_time > 0 else '0000-00-00 00:00:00',
                    nodes_total,
                    nodes_done,
                    nodes_failed,
                    nodes_queued
                ))

        self._mysql.insert_many('dataset_requests', fields, None, data, do_update = True)

        self._mysql.query('DELETE FROM `dataset_requests` WHERE `queue_time` < DATE_SUB(NOW(), INTERVAL 1 YEAR)')
        self._mysql.query('UPDATE `system` SET `dataset_requests_last_update` = NOW()')
