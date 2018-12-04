#! /usr/bin/env python

import unittest

from dynamo.client import inject
from dynamo.client import client
from dynamo import dataformat

class TestInject(unittest.TestCase):
    def test_makedatasetdata(self):
        # Some simple shit
        datasets = inject.make_dataset_data(['/dataset/name/RAW'])
        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0],
                         {'status': 'unknown',
                          'name': '/dataset/name/RAW',
                          'data_type': 'unknown'})

        files = inject.make_dataset_data(['/dataset/name/BLOCKY,nice,test'],
                                         ['abcdefghijk'],
                                         ['/store/mc/file1,100', '/store/mc/file2,200'])

        self.assertEqual(len(files), 1)
        # Copied from a print. Makes sense. Don't change
        self.assertEqual(files,
                         [{'status': 'nice',
                           'blocks': [{'files': [{'name': '/store/mc/file1', 'size': 100},
                                                 {'name': '/store/mc/file2', 'size': 200}],
                                       'name': 'abcdefghijk'}],
                           'name': '/dataset/name/BLOCKY',
                           'data_type': 'test'}])

    def test_makereplicadata(self):
        # I wouldn't call it RuntimeError, but what the heck
        self.assertRaises(RuntimeError, inject.make_dataset_replica_data, ['dataset1', 'dataset2'], ['block'])
        self.assertRaises(RuntimeError, inject.make_dataset_replica_data, ['dataset'], ['block1', 'block2'], ['file.root'])

        replica = inject.make_dataset_replica_data(['SITE:dataset/name,test_group'], ['block1', 'block2'])
        self.assertEqual(replica,
                         [{'growing': True,
                           'blockreplicas': [{'block': 'block1'}, {'block': 'block2'}],
                           'group': 'test_group',
                           'site': 'SITE',
                           'dataset': 'dataset/name'}])

    def test_webclient(self):
        dynclient = client.DynamoWebClient(dataformat.Configuration(url_base='https://localhost/data/inventory', need_auth=False))

# Come back to this later
#        dynclient.make_request('inject',
#                               {'datasetreplica': inject.make_dataset_replica_data(['SITE:dataset/name,test_group'], ['block1', 'block2'])})


if __name__ == '__main__':
    unittest.main()
